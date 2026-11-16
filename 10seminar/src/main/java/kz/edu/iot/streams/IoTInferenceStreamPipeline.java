package kz.edu.iot.streams;

import org.apache.pekko.Done;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Adapter;
import org.apache.pekko.stream.javadsl.Source;
import org.apache.pekko.stream.javadsl.Sink;

import kz.edu.iot.ai.ThreadSafeInferenceCache;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class IoTInferenceStreamPipeline {

    public record RawSensorPacket(
            String sensorId, double[] features, boolean valid) {}

    public record AnomalyAlert(
            String sensorId, double anomalyScore) {}

    public static void main(String[] args) {
        ActorSystem<Void> system =
                ActorSystem.create(Behaviors.empty(), "IoT-Streams");

        ThreadSafeInferenceCache cache =
                new ThreadSafeInferenceCache();

        AtomicInteger validPackets = new AtomicInteger();
        AtomicInteger alerts = new AtomicInteger();
        ExecutorService inferenceExecutor = Executors.newFixedThreadPool(4);
        

        Source<RawSensorPacket, ?> source = Source.range(1, 1000)
                .map(i -> new RawSensorPacket(
                        "SENSOR-" + (i % 10),
                        new double[]{i * 0.1, (i % 5) * 1.5, 42.0},
                        i % 17 != 0
                ));

        CompletionStage<Done> result = source
                .filter(RawSensorPacket::valid)
                .map(packet -> {
                    validPackets.incrementAndGet();
                    return packet;
                })
                .groupedWithin(16, Duration.ofMillis(50))
                .mapAsync(4, batch ->
                        CompletableFuture.supplyAsync(() ->
                                processBatch(batch, cache), inferenceExecutor))
                .mapConcat(list -> list)
                .filter(alert -> alert.anomalyScore() > 0.45)
                .runWith(Sink.foreach(alert -> {
                    alerts.incrementAndGet();
                    System.out.println("ANOMALY: " + alert);
                }), Adapter.toClassic(system));

        try {
            result.toCompletableFuture().join();

            System.out.println("Total packets: 1000");
            System.out.println("Valid packets: " + validPackets.get());
            System.out.println("Invalid packets: " +
                    (1000 - validPackets.get()));
            System.out.println("Anomaly alerts: " + alerts.get());
            System.out.println("Cache hit ratio: " +
                    cache.getCacheHitRatio());

            if (validPackets.get() != 942) {
                throw new IllegalStateException("Unexpected valid packet count");
            }
        } finally {
            inferenceExecutor.shutdown();
            try {
                if (!inferenceExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    inferenceExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                inferenceExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            inferenceExecutor.shutdown();
            try {
                if (!inferenceExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    inferenceExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                inferenceExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            system.terminate();
            system.getWhenTerminated().toCompletableFuture().join();
        }
    }

    private static List<AnomalyAlert> processBatch(
            List<RawSensorPacket> batch,
            ThreadSafeInferenceCache cache) {

        return batch.stream().map(packet -> {
            double[] probs = cache.predictAnomaly(packet.features());
            return new AnomalyAlert(packet.sensorId(), probs[2]);
        }).toList();
    }
}
