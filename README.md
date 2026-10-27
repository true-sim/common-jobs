# common-jobs

Deterministic parallel work for true-sim. Package `com.truesim.common.jobs`.

Two layers:

- **Compute** (`JobSystem`): data-parallel, deterministic, allocation-free work for the frame, such
  as collision and physics.
- **Tasks** (`TaskScheduler`): engine work such as downloads, file access and loading, on lanes of
  virtual or platform threads with limits, priorities, serial ordering and dependencies, handed back
  to the main thread at a point of its choosing.

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

## Tasks: I/O and background work

```java
try (TaskScheduler tasks = new TaskScheduler()) {
    Lane downloads = tasks.virtualLane("downloads", 1); // virtual threads, one download at a time
    Lane files = tasks.serialLane("files");              // one at a time, in order
    Lane decode = tasks.platformLane("decode", 4);       // 4 platform threads

    Task<byte[]> bytes = downloads.submit(() -> fetch(url));
    Task<Image> image = decode.task(() -> decodePng(bytes.result()))
        .after(bytes)                                    // starts once bytes succeeded
        .priority(10)                                    // before other ready tasks of the lane
        .timeout(Duration.ofSeconds(5))                  // cancelled if it runs longer
        .deliver(done -> textures.add(done.result()))    // handed to the main thread
        .submit();
    files.task(() -> save(slot)).serialKey("slot-1").submit(); // one at a time per key

    // Once per frame, on the main thread:
    tasks.drainCompleted(); // runs deliver handlers here, in submission order
}
```

- **Lanes** run at most `maxConcurrent` tasks at once; ready tasks start by priority, then
  submission order.
- **Dependencies:** a task waits for the tasks in `after` and fails if one of them fails or is
  cancelled. A **serial key** only orders: the next task with the key runs after the previous one,
  whatever its outcome (a failed write does not block the next).
- **Cancelling:** `cancel()` drops a task that has not started and interrupts a running one;
  `timeout` cancels it after running too long. `close()` cancels everything unfinished.
- **Hand-over:** `deliver` handlers run only in `drainCompleted()`, on its thread, in submission
  order, so the simulation sees results at a fixed point of the frame.
- **Mixed work:** chains can hop between lanes; for data-parallel work inside a task, use a
  `JobSystem` from a lane with one platform thread (a job system takes one submitter at a time).
- `result()`, `join()`, `await(timeout)`, `state()` and `failure()` inspect tasks from any thread.

## Building

```
./gradlew build
```

To develop against a local build-logic checkout: `./gradlew build -Ptruesim.build-logic=../build-logic`.
The build enforces platform independence, allocation-free code and 100% line and branch coverage.
