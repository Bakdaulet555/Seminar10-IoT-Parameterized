package kz.edu.iot.ai;

import org.junit.jupiter.api.Test;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class ThreadSafeInferenceCacheTest {

    @Test
    void testCacheHit() {
        ThreadSafeInferenceCache cache =
                new ThreadSafeInferenceCache();

        double[] sensor = {10.5, 20.0, 30.5};

        double[] first = cache.predictAnomaly(sensor);
        double[] second = cache.predictAnomaly(sensor);

        assertArrayEquals(first, second);
        assertEquals(2, cache.getTotalRequests());
        assertEquals(0.5, cache.getCacheHitRatio(), 0.001);

        System.out.println("Cache requests: 2");
        System.out.println("Cache hits: 1");
        System.out.println("Cache hit ratio: 50%");
    }

    @Test
    void testConcurrentAccess() throws Exception {
        ThreadSafeInferenceCache cache =
                new ThreadSafeInferenceCache();

        ExecutorService executor = Executors.newFixedThreadPool(16);

        try {
            int requests = 1000;
            CountDownLatch done = new CountDownLatch(requests);

            for (int i = 0; i < requests; i++) {
                final int index = i;
                executor.submit(() -> {
                    try {
                        double[] result = cache.predictAnomaly(
                                new double[]{index % 10, 2.0, 3.0}
                        );

                        assertEquals(3, result.length);
                        assertEquals(1.0,
                                result[0] + result[1] + result[2],
                                0.00001);
                    } finally {
                        done.countDown();
                    }
                });
            }

            assertTrue(done.await(30, TimeUnit.SECONDS));
            assertEquals(requests, cache.getTotalRequests());

            System.out.println("Concurrent requests: " + requests);
            System.out.println("Cache hit ratio: " +
                    cache.getCacheHitRatio());
        } finally {
            executor.shutdownNow();
        }
    }
}
