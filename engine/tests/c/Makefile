CC ?= cc
CFLAGS = -std=c99 -Wall -Wextra -Werror -I ../../citus/src/include
SOURCES = ../../citus/src/backend/distributed/observer/obs_sketch.c ../../citus/src/backend/distributed/observer/obs_ring.c test_observer.c

.PHONY: test clean
test: observer_tests
	./observer_tests

observer_tests: $(SOURCES)
	$(CC) $(CFLAGS) -pthread $(SOURCES) -o $@

clean:
	rm -f observer_tests
