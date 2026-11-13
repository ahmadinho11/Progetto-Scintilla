CC = gcc
CFLAGS = -Wall -Wextra -pthread -std=c11 -O2 -Isrc
LDFLAGS = -pthread -lrt

SRC_DIR = src
BINARIES = scheduler report logger

.PHONY: all clean test run-all

all: $(BINARIES)

scheduler: $(SRC_DIR)/scheduler.c $(SRC_DIR)/common.h
	$(CC) $(CFLAGS) -o $@ $(SRC_DIR)/scheduler.c $(LDFLAGS)

report: $(SRC_DIR)/report.c $(SRC_DIR)/common.h
	$(CC) $(CFLAGS) -o $@ $(SRC_DIR)/report.c $(LDFLAGS)

logger: $(SRC_DIR)/logger.c $(SRC_DIR)/common.h
	$(CC) $(CFLAGS) -o $@ $(SRC_DIR)/logger.c $(LDFLAGS)

clean:
	rm -f $(BINARIES) *.o schedule.log

test: all
	@echo "=== Running Test: Round Robin (quantum 2) on jobs1.txt ==="
	./scheduler inputs/jobs1.txt rr 2

run-all: all
	@echo "=== Running ALL Algorithms on jobs1.txt ==="
	./scheduler inputs/jobs1.txt all 2
