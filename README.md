# common-jobs

Deterministic parallel work for true-sim. Package `com.truesim.common.jobs`.

Status: the compute layer (`JobSystem`). Planned next: a task layer for engine work such as I/O, with
lanes (virtual or platform threads, concurrency limits, serial ordering), dependencies between
tasks, and hand-over of results to the main thread at frame boundaries.

## Usage

```kotlin
dependencies {
    implementation("com.truesim.common:common-jobs:0.1.0")
}
```

```java
try (JobSystem jobs = JobSystem.ofAvailableProcessors()) {
    // Every index in 0..n, in chunks of 256, on all threads; returns when done.
    jobs.parallelFor(n, 256, (from, to, worker) -> {
        for (int i = from; i < to; i++) {
            out[i] = f(in[i]);
        }
    });
}
```

## Deterministic on any number of threads

- Chunk boundaries depend only on `count` and `grain`, never on the number of threads; every chunk
  runs exactly once.
- Threads claim chunks as they become free, which balances uneven work, so *which* thread runs a
  chunk varies. Results stay identical when tasks write by index, and when per-chunk partial results
  are combined in chunk order:

```java
double[] partial = new double[JobSystem.chunkCount(n, 256)];
jobs.parallelChunks(n, 256, (chunk, from, to, worker) -> partial[chunk] = sum(values, from, to));
double total = 0;
for (double p : partial) {
    total += p; // bit for bit the same on 1, 2, 3 or 100 threads
}
```

- Tasks get a worker index from 0 to `threads() - 1` for per-thread contexts (scratch buffers,
  query objects); the calling thread is worker 0 and works too.

## Details

- `new JobSystem(threads)` starts `threads - 1` workers; with 1, everything runs on the caller.
  Idle workers spin briefly, then park until the next job.
- Submitting a job allocates nothing; create task objects once and reuse them.
- Jobs are submitted from one thread at a time. A job started from inside a task runs inline on
  that thread, with that thread's worker index.
- If a task throws, no further chunks start and the first exception is rethrown to the caller; the
  system stays usable.
- `close()` stops the workers.

## Building

```
./gradlew build
```

To develop against a local build-logic checkout: `./gradlew build -Ptruesim.build-logic=../build-logic`.
The build enforces platform independence, allocation-free code and 100% line and branch coverage.
