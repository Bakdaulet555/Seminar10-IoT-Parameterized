package kz.edu.iot.sensors;

import org.junit.jupiter.api.Test;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;

class SafeSensorListenerTest {

    @Test
    void testSensorShutdown() {
        AtomicInteger received = new AtomicInteger(0);

        SafeSensorListener.SensorBus bus =
                (topic, listener) ->
                        System.out.println("Registered: " + topic);

        SafeSensorListener listener =
                SafeSensorListener.createAndRegister(
                        bus,
                        "temperature",
                        value -> received.incrementAndGet()
                );

        listener.onDataReceived(25.5);
        listener.onDataReceived(30.0);

        assertEquals(2, received.get());

        listener.requestShutdown();

        listener.onDataReceived(40.0);

        assertEquals(2, received.get());
        assertTrue(listener.isShutdownRequested());

        System.out.println("Received before shutdown: 2");
        System.out.println("Received after shutdown: 2");
        System.out.println("Shutdown successful");
    }

    @Test
    void testSafePublication() {
        AtomicReference<SafeSensorListener> registered =
                new AtomicReference<>();

        SafeSensorListener.SensorBus bus = (topic, listener) -> {
            assertEquals("temperature", topic);
            assertEquals("temperature", listener.getSensorTopic());
            registered.set(listener);
        };

        SafeSensorListener listener =
                SafeSensorListener.createAndRegister(
                        bus, "temperature", value -> {}
                );

        assertSame(listener, registered.get());

        System.out.println("Safe publication: OK");
    }
}
