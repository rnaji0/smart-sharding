# Smart Sharding

## Engine collector: status

The Citus fork (`engine/citus`, branch `smart-sharding/v14.2.0` of `rnaji0/citus`) adds the
`citus_observer` extension: per-task and per-shard execution statistics, planner decisions and column
usage, hot primary keys, distributed transaction traces with shard-group/tenant co-access, sampled lock
contention resolved to rows, per-tenant row changes decoded from the WAL, and shard move/split phase
events. `engine/collector` polls all of it, plus stock Citus/Postgres views, into DuckDB.

```sh
make engine-deploy     # build the image from the fork and restart the cluster
make engine-test-c     # sketch and event ring unit tests
make collector-test    # collector unit tests
make engine-test-sql   # citus_observer integration tests (running cluster)
make collector-run ARGS="--duration 60"
```

### Overhead: budget not met yet

Budget: under 2% throughput loss and under 3% p99 increase with every area on. Measured with
`engine/bench/overhead.py` (3 off/on pairs, 60 s each, simulator as load generator; "on" includes change
capture and the collector polling every 5 s), on a laptop Docker cluster (coordinator + 3 workers):

| Load | Metric | Off | On | Change |
|---|---|---|---|---|
| Light, ~100 txn/s | throughput | 100.4 txn/s | 100.9 txn/s | +0.5% |
| | p99 | 1070 ms | 262 ms | inconclusive: off runs alone ranged 68–2066 ms |
| Saturating, 3× offered | throughput | 519.9 txn/s | 447.4 txn/s | −13.9% raw |
| | p99 | 998 ms | 1540 ms | +54% raw |

Every saturation run was about 15% slower than the one before it, whichever arm it was
(577 → 531 → 535 → 449 → 448 → 362 txn/s), so the raw numbers mostly measure drift. Comparing each
"on" run with the mean of the "off" runs around it gives **−4.5% and −8.6% throughput**: over budget,
but not measured precisely.

Open problems:

1. **The benchmark drifts.** Throughput falls run over run at saturation; the cause (data growth, bloat,
   thermal throttling) is unknown. Needs ABBA ordering, data reset or a non-growing workload, and longer
   runs before numbers can be trusted.
2. **Engine and collector cost are not separated.** "On" bundles the in-engine recording with the
   collector's polling. Each area (`citus_observer.track_*`) and the collector need their own arm.
3. **Likely costs to tune once measured:**
   - the contention sampler reads the whole lock table every 20 ms on every node;
   - every distributed transaction writes a `txn` event under one exclusive lock
     (`citus_observer.txn_event_sample_rate` defaults to 1.0);
   - the collector reads `citus_shards` (shard sizes from every node) and decodes the WAL each poll.

Not yet verified: bringing up a fresh cluster with `make reset` (the running cluster had the extension
added by hand).
