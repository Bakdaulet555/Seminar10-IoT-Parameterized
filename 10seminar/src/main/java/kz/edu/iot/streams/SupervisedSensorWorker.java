package kz.edu.iot.streams;

import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.SupervisorStrategy;
import org.apache.pekko.actor.typed.javadsl.Behaviors;

import java.time.Duration;

public class SupervisedSensorWorker {

    public interface Command {}

    public record ProcessPacket(int packetId) implements Command {}

    public static Behavior<Command> create() {
        return Behaviors.supervise(
                Behaviors.<Command>receive((context, message) -> {
                    if (message instanceof ProcessPacket packet) {
                        if (packet.packetId() < 0) {
                            throw new IllegalArgumentException(
                                    "Invalid packet ID");
                        }

                        context.getLog().info(
                                "Processed packet: {}", packet.packetId());
                    }

                    return Behaviors.same();
                })
        ).onFailure(
                IllegalArgumentException.class,
                SupervisorStrategy.restartWithBackoff(
                        Duration.ofMillis(200),
                        Duration.ofSeconds(5),
                        0.2
                )
        );
    }
}
