package kz.edu.iot.concurrency;

import org.junit.jupiter.api.Test;
import java.util.Set;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class TelemetrySequenceGeneratorTest {

    @Test
    void testAtomicGenerator() throws Exception {
        runTest(false);
    }

    @Test
    void testSynchronizedGenerator() throws Exception {
        runTest(true);
    }

    private void runTest(boolean synchronizedMode) throws Exception {
        TelemetrySequenceGenerator generator =
                new TelemetrySequenceGenerator("GATEWAY-01");

        int threads = 32;
        int packetsPerThread = 1000;
        Set<String> ids = ConcurrentHashMap.newKeySet();

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);

        try {
            for (int i = 0; i < threads; i++) {
                executor.submit(() -> {
                    try {
                        start.await();
                        for (int j = 0; j < packetsPerThread; j++) {
                            String id = synchronizedMode
                                    ? generator.nextPacketIdSynchronized()
                                    : generator.nextPacketId();
                            ids.add(id);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }

            start.countDown();
            assertTrue(done.await(30, TimeUnit.SECONDS));

            assertEquals(32000, ids.size());
            assertEquals(32000, generator.getCurrentCount());

            System.out.println("Mode: " +
                    (synchronizedMode ? "Synchronized" : "AtomicLong"));
            System.out.println("Threads: " + threads);
            System.out.println("Unique IDs: " + ids.size());
            System.out.println("Collisions: " +
                    (generator.getCurrentCount() - ids.size()));
        } finally {
            executor.shutdownNow();
        }
    }
}
