package kz.edu.iot.akka;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.*;

import java.util.ArrayDeque;
import java.util.Deque;

public class EdgeTelemetryActor
        extends AbstractBehavior<EdgeTelemetryActor.Command> {

    public sealed interface Command
            permits RecordMeasurement, UpdateThreshold, GetAverage {}

    public record RecordMeasurement(double value, long timestamp)
            implements Command {}

    public record UpdateThreshold(double newThreshold)
            implements Command {}

    public record GetAverage(ActorRef<Double> replyTo)
            implements Command {}

    private final String deviceId;
    private final int windowSize;
    private final Deque<Double> measurements = new ArrayDeque<>();
    private double threshold;

    public static Behavior<Command> create(
            String deviceId, int windowSize, double threshold) {
        if (windowSize <= 0) {
            throw new IllegalArgumentException("Window size must be positive");
        }
        return Behaviors.setup(ctx ->
                new EdgeTelemetryActor(ctx, deviceId, windowSize, threshold));
    }

    private EdgeTelemetryActor(
            ActorContext<Command> context,
            String deviceId,
            int windowSize,
            double threshold) {
        super(context);
        this.deviceId = deviceId;
        this.windowSize = windowSize;
        this.threshold = threshold;
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(RecordMeasurement.class, this::record)
                .onMessage(UpdateThreshold.class, this::updateThreshold)
                .onMessage(GetAverage.class, this::getAverage)
                .build();
    }

    private Behavior<Command> record(RecordMeasurement msg) {
        if (measurements.size() == windowSize) {
            measurements.removeFirst();
        }

        measurements.addLast(msg.value());

        double avg = average();

        getContext().getLog().info(
                "Device {} temperature: {}, average: {}",
                deviceId, msg.value(), avg);

        if (avg > threshold) {
            getContext().getLog().warn(
                    "OVERHEAT! Device {} average {} > threshold {}",
                    deviceId, avg, threshold);
        }

        return this;
    }

    private Behavior<Command> updateThreshold(UpdateThreshold msg) {
        threshold = msg.newThreshold();
        getContext().getLog().info("New threshold: {}", threshold);
        return this;
    }

    private Behavior<Command> getAverage(GetAverage msg) {
        msg.replyTo().tell(average());
        return this;
    }

    private double average() {
        return measurements.stream()
                .mapToDouble(Double::doubleValue)
                .average()
                .orElse(0.0);
    }
}
