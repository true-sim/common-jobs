package com.truesim.common.jobs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class LaneSpecTest {

    /** Global definitions, as an engine would keep them. */
    static final class Lanes {
        static final LaneSpec DOWNLOADS = LaneSpec.virtual("downloads", 1);
        static final LaneSpec FILES = LaneSpec.serial("files");
        static final LaneSpec DECODE = LaneSpec.platform("decode", 3);
    }

    @Test
    void definitions() {
        assertThat(Lanes.DOWNLOADS.name()).isEqualTo("downloads");
        assertThat(Lanes.DOWNLOADS.isVirtual()).isTrue();
        assertThat(Lanes.DOWNLOADS.maxConcurrent()).isEqualTo(1);
        assertThat(Lanes.FILES.isVirtual()).isTrue();
        assertThat(Lanes.FILES.maxConcurrent()).isEqualTo(1);
        assertThat(Lanes.DECODE.isVirtual()).isFalse();
        assertThat(Lanes.DECODE.maxConcurrent()).isEqualTo(3);
        assertThat(Lanes.DECODE.toString()).isEqualTo("LaneSpec decode (platform, 3)");
        assertThat(Lanes.DOWNLOADS.toString()).isEqualTo("LaneSpec downloads (virtual, 1)");
        assertThatIllegalArgumentException().isThrownBy(() -> LaneSpec.virtual("none", 0));
        assertThatIllegalArgumentException().isThrownBy(() -> LaneSpec.platform("none", -1));
    }

    @Test
    void eachSchedulerMakesItsOwnLanes() throws InterruptedException {
        try (TaskScheduler game = new TaskScheduler(); TaskScheduler editor = new TaskScheduler()) {
            Lane downloads = game.lane(Lanes.DOWNLOADS);
            assertThat(game.lane(Lanes.DOWNLOADS)).isSameAs(downloads); // created once, then reused
            assertThat(editor.lane(Lanes.DOWNLOADS)).isNotSameAs(downloads); // separate threads per scheduler
            assertThat(downloads.name()).isEqualTo("downloads");
            assertThat(downloads.maxConcurrent()).isEqualTo(1);
            // The spec object is the key: an equal name is a different lane.
            LaneSpec lookalike = LaneSpec.virtual("downloads", 1);
            assertThat(game.lane(lookalike)).isNotSameAs(downloads);
            // The lanes run on the threads the specs describe.
            Task<Boolean> virtual = game.lane(Lanes.FILES).submit(() -> Thread.currentThread().isVirtual());
            Task<Boolean> platform = game.lane(Lanes.DECODE).submit(() -> Thread.currentThread().isVirtual());
            assertThat(virtual.join()).isTrue();
            assertThat(platform.join()).isFalse();
            // One download at a time, whoever submits.
            AtomicInteger concurrent = new AtomicInteger();
            AtomicInteger maximum = new AtomicInteger();
            Task<?>[] fetched = new Task<?>[5];
            for (int i = 0; i < fetched.length; i++) {
                fetched[i] = game.lane(Lanes.DOWNLOADS).submit(() -> {
                    maximum.accumulateAndGet(concurrent.incrementAndGet(), Math::max);
                    Thread.sleep(3);
                    return concurrent.decrementAndGet();
                });
            }
            for (Task<?> task : fetched) {
                task.await();
            }
            assertThat(maximum.get()).isEqualTo(1);
        }
    }

    @Test
    void closedSchedulersMakeNoLanes() {
        TaskScheduler tasks = new TaskScheduler();
        tasks.close();
        assertThatIllegalStateException().isThrownBy(() -> tasks.lane(Lanes.FILES));
    }
}
