package kz.edu.iot.akka;

import org.apache.pekko.actor.typed.ActorSystem;

public class ActorDemo {

    public static void main(String[] args) throws Exception {
        ActorSystem<EdgeTelemetryActor.Command> system =
                ActorSystem.create(
                        EdgeTelemetryActor.create("SENSOR-01", 3, 40.0),
                        "IoT-Telemetry-System"
                );

        system.tell(new EdgeTelemetryActor.RecordMeasurement(25, 1));
        system.tell(new EdgeTelemetryActor.RecordMeasurement(35, 2));
        system.tell(new EdgeTelemetryActor.RecordMeasurement(70, 3));
        system.tell(new EdgeTelemetryActor.RecordMeasurement(80, 4));

        Thread.sleep(2000);

        system.terminate();
        system.getWhenTerminated().toCompletableFuture().join();
    }
}
