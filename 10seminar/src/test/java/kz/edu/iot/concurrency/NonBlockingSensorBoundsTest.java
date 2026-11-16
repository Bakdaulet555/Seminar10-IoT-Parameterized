package kz.edu.iot.concurrency;

import org.junit.jupiter.api.Test;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class NonBlockingSensorBoundsTest {

    @Test
    void testConcurrentMinMax() throws Exception {
        NonBlockingSensorBounds tracker =
                new NonBlockingSensorBounds();

        int threads = 32;
        ExecutorService executor =
                Executors.newFixedThreadPool(threads);

        CountDownLatch done = new CountDownLatch(threads);

        try {
            for (int i = 0; i < threads; i++) {
                final int index = i;

                executor.submit(() -> {
                    try {
                        for (int j = 0; j < 1000; j++) {
                            double value = index * 1000 + j;
                            tracker.updateReading(value);
                        }
                    } finally {
                        done.countDown();
                    }
                });
            }

            assertTrue(done.await(30, TimeUnit.SECONDS));

            NonBlockingSensorBounds.Bounds result =
                    tracker.getBounds();

            assertNotNull(result);
            assertEquals(0.0, result.min());
            assertEquals(31999.0, result.max());
            assertTrue(result.min() <= result.max());

            System.out.println("Threads: 32");
            System.out.println("Readings: 32000");
            System.out.println("Minimum: " + result.min());
            System.out.println("Maximum: " + result.max());
            System.out.println("CAS invariant: OK");
        } finally {
            executor.shutdownNow();
        }
    }
}
