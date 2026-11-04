/*
 * test_observer.c
 *	  Unit tests for the observer's count-min/top-K sketch and event ring.
 *
 * Both modules are plain C, so they are compiled here without PostgreSQL.
 * Run with: make -C engine/tests/c test
 */
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "distributed/observer/obs_ring.h"
#include "distributed/observer/obs_sketch.h"

static int failures = 0;
static int checks = 0;

#define CHECK(cond) \
	do { \
		checks++; \
		if (!(cond)) \
		{ \
			failures++; \
			fprintf(stderr, "%s:%d: CHECK(%s) failed\n", __FILE__, __LINE__, #cond); \
		} \
	} while (0)

#define RUN(test) \
	do { \
		int before = failures; \
		test(); \
		printf("%s %s\n", failures == before ? "ok  " : "FAIL", #test); \
	} while (0)


static ObsSketch *
new_sketch(uint32_t depth, uint32_t width, uint32_t k)
{
	ObsSketch *sketch = malloc(obs_sketch_size(depth, width, k));

	obs_sketch_init(sketch, depth, width, k);
	return sketch;
}


static uint64_t
key_hash(int key)
{
	return obs_hash64(&key, sizeof(key), 0);
}


/* deterministic xorshift so failures reproduce */
static uint64_t rng_state = 88172645463325252ULL;

static uint64_t
next_random(void)
{
	rng_state ^= rng_state << 13;
	rng_state ^= rng_state >> 7;
	rng_state ^= rng_state << 17;
	return rng_state;
}


/* Zipf-like: key i drawn with weight 1/(i+1) */
static int
skewed_key(int distinct, const double *cumulative)
{
	double target = (double) (next_random() % 1000000) / 1000000.0 * cumulative[distinct - 1];
	int low = 0;
	int high = distinct - 1;

	while (low < high)
	{
		int mid = (low + high) / 2;

		if (cumulative[mid] < target)
		{
			low = mid + 1;
		}
		else
		{
			high = mid;
		}
	}
	return low;
}


static void
test_sketch_size_rounds_width_and_keeps_entries_aligned(void)
{
	CHECK(obs_sketch_size(4, 1000, 8) == obs_sketch_size(4, 1024, 8));
	CHECK(obs_sketch_size(4, 1024, 8) < obs_sketch_size(4, 1025, 8));
	CHECK(obs_sketch_size(0, 0, 0) == obs_sketch_size(1, 1, 0));

	ObsSketch *sketch = new_sketch(3, 5, 4);
	CHECK(sketch->width == 8 && sketch->depth == 3 && sketch->k == 4);

	/* an odd counter count must not misalign the top-K entries */
	CHECK(obs_sketch_offer(sketch, 7, 1, 1, OBS_ACCESS_READ, "a") == 1);
	ObsTopKEntry out[4];
	CHECK(obs_sketch_topk(sketch, out, 4) == 1 && out[0].hash == 7);
	free(sketch);
}


static void
test_sketch_starts_empty_and_resets(void)
{
	ObsSketch *sketch = new_sketch(4, 64, 4);
	ObsTopKEntry out[4];

	CHECK(obs_sketch_total(sketch) == 0);
	CHECK(obs_sketch_estimate(sketch, key_hash(1)) == 0);
	CHECK(obs_sketch_topk(sketch, out, 4) == 0);

	obs_sketch_offer(sketch, key_hash(1), obs_sketch_increment(sketch, key_hash(1)), 1, 0, "k");
	obs_sketch_reset(sketch);
	CHECK(obs_sketch_total(sketch) == 0 && sketch->used == 0);
	CHECK(obs_sketch_estimate(sketch, key_hash(1)) == 0);
	CHECK(obs_sketch_topk(sketch, out, 4) == 0);
	free(sketch);
}


static void
test_sketch_estimates_never_undercount_and_stay_near_truth(void)
{
	enum { DISTINCT = 5000, EVENTS = 100000 };
	ObsSketch *sketch = new_sketch(4, 4096, 16);
	static uint32_t exact[DISTINCT];
	static double cumulative[DISTINCT];
	double sum = 0;

	for (int i = 0; i < DISTINCT; i++)
	{
		sum += 1.0 / (i + 1);
		cumulative[i] = sum;
	}

	for (int i = 0; i < EVENTS; i++)
	{
		int key = skewed_key(DISTINCT, cumulative);
		uint64_t before = obs_sketch_estimate(sketch, key_hash(key));
		uint64_t after = obs_sketch_increment(sketch, key_hash(key));

		CHECK(after >= before + 1 || before == 0);
		exact[key]++;
	}
	CHECK(obs_sketch_total(sketch) == EVENTS);

	/* error bound: e * total / width with probability 1 - e^-depth */
	uint64_t bound = (uint64_t) (2.72 * EVENTS / 4096) + 1;
	int over_bound = 0;
	for (int key = 0; key < DISTINCT; key++)
	{
		uint64_t estimate = obs_sketch_estimate(sketch, key_hash(key));

		CHECK(estimate >= exact[key]);
		if (estimate - exact[key] > bound)
		{
			over_bound++;
		}
	}
	CHECK(over_bound < DISTINCT / 50);
	free(sketch);
}


static void
test_sketch_wants_keys_while_top_k_has_room(void)
{
	ObsSketch *sketch = new_sketch(2, 64, 2);

	CHECK(obs_sketch_wants(sketch, 1));
	obs_sketch_offer(sketch, 1, 5, 1, 0, "a");
	CHECK(obs_sketch_wants(sketch, 1));
	obs_sketch_offer(sketch, 2, 9, 1, 0, "b");

	/* full: only estimates above the minimum (5) qualify */
	CHECK(!obs_sketch_wants(sketch, 5));
	CHECK(obs_sketch_wants(sketch, 6));
	free(sketch);

	ObsSketch *empty = new_sketch(2, 64, 0);
	CHECK(!obs_sketch_wants(empty, 100));
	CHECK(obs_sketch_offer(empty, 1, 100, 1, 0, "a") == 0);
	free(empty);
}


static void
test_sketch_offer_admits_raises_and_replaces_the_minimum(void)
{
	ObsSketch *sketch = new_sketch(2, 64, 2);
	ObsTopKEntry out[2];

	CHECK(obs_sketch_offer(sketch, 10, 3, 100, OBS_ACCESS_READ, "a") == 1);
	CHECK(obs_sketch_offer(sketch, 20, 7, 200, OBS_ACCESS_WRITE, "b") == 1);

	/* raising an existing key; a lower estimate is ignored */
	CHECK(obs_sketch_offer(sketch, 10, 8, 100, OBS_ACCESS_READ, "a") == 1);
	CHECK(obs_sketch_offer(sketch, 10, 2, 100, OBS_ACCESS_READ, "a") == 0);

	/* new key that does not beat the minimum (7) is rejected */
	CHECK(obs_sketch_offer(sketch, 30, 7, 300, OBS_ACCESS_READ, "c") == 0);

	/* one that does replaces it and inherits its count as error */
	CHECK(obs_sketch_offer(sketch, 30, 12, 300, OBS_ACCESS_READ, "c") == 1);
	CHECK(obs_sketch_topk(sketch, out, 2) == 2);
	CHECK(out[0].hash == 30 && out[0].count == 12 && out[0].error == 7 && out[0].shardid == 300);
	CHECK(out[1].hash == 10 && out[1].count == 8 && out[1].error == 0);
	CHECK(strcmp(out[1].key, "a") == 0 && out[1].access == OBS_ACCESS_READ);

	/* hash 0 marks empty slots and is never admitted */
	CHECK(obs_sketch_offer(sketch, 0, 100, 1, 0, "zero") == 0);
	free(sketch);
}


static void
test_sketch_truncates_long_keys(void)
{
	ObsSketch *sketch = new_sketch(2, 64, 1);
	char longKey[OBS_SKETCH_KEY_LEN * 2];
	ObsTopKEntry out[1];

	memset(longKey, 'x', sizeof(longKey) - 1);
	longKey[sizeof(longKey) - 1] = '\0';
	obs_sketch_offer(sketch, 1, 1, 1, 0, longKey);
	obs_sketch_topk(sketch, out, 1);
	CHECK(strlen(out[0].key) == OBS_SKETCH_KEY_LEN - 1);

	obs_sketch_offer(sketch, 2, 5, 1, 0, NULL);
	obs_sketch_topk(sketch, out, 1);
	CHECK(out[0].hash == 2 && out[0].key[0] == '\0');
	free(sketch);
}


static void
test_sketch_topk_is_sorted_and_bounded(void)
{
	ObsSketch *sketch = new_sketch(2, 64, 8);
	ObsTopKEntry out[8];
	uint64_t counts[] = { 4, 9, 1, 7, 3, 8 };

	for (int i = 0; i < 6; i++)
	{
		obs_sketch_offer(sketch, (uint64_t) i + 1, counts[i], 1, 0, "k");
	}
	CHECK(obs_sketch_topk(sketch, out, 8) == 6);
	for (int i = 1; i < 6; i++)
	{
		CHECK(out[i - 1].count >= out[i].count);
	}
	CHECK(obs_sketch_topk(sketch, out, 3) == 3);
	free(sketch);
}


static void
test_sketch_finds_the_true_heavy_hitters(void)
{
	enum { DISTINCT = 20000, EVENTS = 200000, K = 32, TOP = 10 };
	ObsSketch *sketch = new_sketch(4, 8192, K);
	static double cumulative[DISTINCT];
	double sum = 0;

	for (int i = 0; i < DISTINCT; i++)
	{
		sum += 1.0 / (i + 1);
		cumulative[i] = sum;
	}

	/* the same flow the engine uses: increment, then offer if wanted */
	for (int i = 0; i < EVENTS; i++)
	{
		int key = skewed_key(DISTINCT, cumulative);
		uint64_t hash = key_hash(key);
		uint64_t estimate = obs_sketch_increment(sketch, hash);

		if (obs_sketch_wants(sketch, estimate))
		{
			char text[32];

			snprintf(text, sizeof(text), "id=%d", key);
			obs_sketch_offer(sketch, hash, estimate, 1, 0, text);
		}
	}

	ObsTopKEntry out[K];
	int count = obs_sketch_topk(sketch, out, K);
	CHECK(count == K);
	for (int key = 0; key < TOP; key++)
	{
		char text[32];
		int found = 0;

		snprintf(text, sizeof(text), "id=%d", key);
		for (int i = 0; i < count; i++)
		{
			found |= strcmp(out[i].key, text) == 0;
		}
		CHECK(found);
	}
	CHECK(strcmp(out[0].key, "id=0") == 0);
	free(sketch);
}


static void
test_hash_never_returns_zero_and_depends_on_seed(void)
{
	int same = 0;

	for (int i = 0; i < 100000; i++)
	{
		CHECK(obs_hash64(&i, sizeof(i), 0) != 0);
		same += obs_hash64(&i, sizeof(i), 0) == obs_hash64(&i, sizeof(i), 1);
	}
	CHECK(same == 0);
	CHECK(obs_hash64("", 0, 0) != 0);
	CHECK(obs_hash64("abc", 3, 5) == obs_hash64("abc", 3, 5));
}


typedef struct ThreadArg
{
	ObsSketch *sketch;
	uint64_t hash;
	int increments;
} ThreadArg;

static void *
increment_many(void *arg)
{
	ThreadArg *threadArg = arg;

	for (int i = 0; i < threadArg->increments; i++)
	{
		obs_sketch_increment(threadArg->sketch, threadArg->hash);
	}
	return NULL;
}


static void
test_sketch_increments_are_exact_under_concurrency(void)
{
	enum { THREADS = 4, INCREMENTS = 50000 };
	ObsSketch *sketch = new_sketch(4, 1024, 4);
	pthread_t threads[THREADS];
	ThreadArg arg = { sketch, key_hash(42), INCREMENTS };

	for (int i = 0; i < THREADS; i++)
	{
		pthread_create(&threads[i], NULL, increment_many, &arg);
	}
	for (int i = 0; i < THREADS; i++)
	{
		pthread_join(threads[i], NULL);
	}
	CHECK(obs_sketch_total(sketch) == THREADS * INCREMENTS);
	CHECK(obs_sketch_estimate(sketch, key_hash(42)) == THREADS * INCREMENTS);
	free(sketch);
}


static ObsRing *
new_ring(uint32_t slots)
{
	ObsRing *ring = malloc(obs_ring_size(slots));

	obs_ring_init(ring, slots);
	return ring;
}


static void
test_ring_starts_empty(void)
{
	ObsRing *ring = new_ring(4);
	uint64_t lost = 99;

	CHECK(obs_ring_head(ring) == 0 && obs_ring_oldest(ring) == 0);
	CHECK(obs_ring_next(ring, 0, &lost) == 0 && lost == 0);
	CHECK(obs_ring_get(ring, 0) == NULL && obs_ring_get(ring, 1) == NULL);
	free(ring);
}


static void
test_ring_assigns_sequential_seqs_and_stores_events(void)
{
	ObsRing *ring = new_ring(4);

	CHECK(obs_ring_push(ring, 100, "txn", "{\"a\": 1}", 8) == 1);
	CHECK(obs_ring_push(ring, 200, "action_end", "{}", 2) == 2);

	const ObsRingSlot *slot = obs_ring_get(ring, 1);
	CHECK(slot != NULL && slot->seq == 1 && slot->ts_us == 100);
	CHECK(strcmp(slot->kind, "txn") == 0 && strcmp(slot->payload, "{\"a\": 1}") == 0 && slot->len == 8);
	CHECK(obs_ring_get(ring, 3) == NULL);
	CHECK(obs_ring_head(ring) == 2 && obs_ring_oldest(ring) == 1);
	free(ring);
}


static void
test_ring_overwrites_the_oldest_events(void)
{
	ObsRing *ring = new_ring(3);

	for (int i = 0; i < 5; i++)
	{
		obs_ring_push(ring, i, "e", "x", 1);
	}
	CHECK(obs_ring_head(ring) == 5 && obs_ring_oldest(ring) == 3);
	CHECK(obs_ring_get(ring, 1) == NULL && obs_ring_get(ring, 2) == NULL);
	CHECK(obs_ring_get(ring, 3) != NULL && obs_ring_get(ring, 5)->seq == 5);
	free(ring);
}


static void
test_ring_next_reports_events_lost_to_overwrite(void)
{
	ObsRing *ring = new_ring(3);
	uint64_t lost = 0;

	for (int i = 0; i < 2; i++)
	{
		obs_ring_push(ring, i, "e", "x", 1);
	}
	CHECK(obs_ring_next(ring, 0, &lost) == 1 && lost == 0);
	CHECK(obs_ring_next(ring, 1, &lost) == 2 && lost == 0);
	CHECK(obs_ring_next(ring, 2, &lost) == 0);
	CHECK(obs_ring_next(ring, 9, &lost) == 0);

	for (int i = 0; i < 6; i++)
	{
		obs_ring_push(ring, i, "e", "x", 1);
	}

	/* head is 8 and 6..8 remain: a reader at 2 missed 3, 4 and 5 */
	CHECK(obs_ring_next(ring, 2, &lost) == 6 && lost == 3);
	CHECK(obs_ring_next(ring, 6, &lost) == 7 && lost == 0);
	CHECK(obs_ring_next(ring, 0, NULL) == 6);
	free(ring);
}


static void
test_ring_truncates_payload_and_kind(void)
{
	ObsRing *ring = new_ring(2);
	char payload[OBS_RING_PAYLOAD_LEN + 100];
	char kind[OBS_RING_KIND_LEN + 10];

	memset(payload, 'p', sizeof(payload));
	memset(kind, 'k', sizeof(kind) - 1);
	kind[sizeof(kind) - 1] = '\0';

	obs_ring_push(ring, 0, kind, payload, sizeof(payload));
	const ObsRingSlot *slot = obs_ring_get(ring, 1);
	CHECK(slot->len == OBS_RING_PAYLOAD_LEN - 1 && strlen(slot->payload) == OBS_RING_PAYLOAD_LEN - 1);
	CHECK(strlen(slot->kind) == OBS_RING_KIND_LEN - 1);

	obs_ring_push(ring, 0, NULL, NULL, 50);
	slot = obs_ring_get(ring, 2);
	CHECK(slot->len == 0 && slot->payload[0] == '\0' && slot->kind[0] == '\0');
	free(ring);
}


static void
test_ring_with_zero_slots_keeps_one(void)
{
	ObsRing *ring = new_ring(0);

	CHECK(ring->nslots == 1);
	obs_ring_push(ring, 0, "e", "a", 1);
	obs_ring_push(ring, 0, "e", "b", 1);
	CHECK(obs_ring_get(ring, 1) == NULL && strcmp(obs_ring_get(ring, 2)->payload, "b") == 0);
	free(ring);
}


int
main(void)
{
	RUN(test_sketch_size_rounds_width_and_keeps_entries_aligned);
	RUN(test_sketch_starts_empty_and_resets);
	RUN(test_sketch_estimates_never_undercount_and_stay_near_truth);
	RUN(test_sketch_wants_keys_while_top_k_has_room);
	RUN(test_sketch_offer_admits_raises_and_replaces_the_minimum);
	RUN(test_sketch_truncates_long_keys);
	RUN(test_sketch_topk_is_sorted_and_bounded);
	RUN(test_sketch_finds_the_true_heavy_hitters);
	RUN(test_hash_never_returns_zero_and_depends_on_seed);
	RUN(test_sketch_increments_are_exact_under_concurrency);
	RUN(test_ring_starts_empty);
	RUN(test_ring_assigns_sequential_seqs_and_stores_events);
	RUN(test_ring_overwrites_the_oldest_events);
	RUN(test_ring_next_reports_events_lost_to_overwrite);
	RUN(test_ring_truncates_payload_and_kind);
	RUN(test_ring_with_zero_slots_keeps_one);

	printf("%d checks, %d failed\n", checks, failures);
	return failures == 0 ? 0 : 1;
}
