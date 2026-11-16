package kz.edu.iot.sensors;

import java.util.Objects;
import java.util.function.Consumer;

public final class SafeSensorListener {

    private final String sensorTopic;
    private final Consumer<Double> telemetryHandler;
    private volatile boolean shutdownRequested = false;

    private SafeSensorListener(
            String sensorTopic,
            Consumer<Double> telemetryHandler) {
        this.sensorTopic = Objects.requireNonNull(sensorTopic);
        this.telemetryHandler = Objects.requireNonNull(telemetryHandler);
    }

    public static SafeSensorListener createAndRegister(
            SensorBus bus,
            String topic,
            Consumer<Double> handler) {

        SafeSensorListener listener =
                new SafeSensorListener(topic, handler);

        bus.register(topic, listener);
        return listener;
    }

    public void onDataReceived(double value) {
        if (!shutdownRequested) {
            telemetryHandler.accept(value);
        }
    }

    public void requestShutdown() {
        shutdownRequested = true;
    }

    public boolean isShutdownRequested() {
        return shutdownRequested;
    }

    public String getSensorTopic() {
        return sensorTopic;
    }

    public interface SensorBus {
        void register(String topic, SafeSensorListener listener);
    }
}
