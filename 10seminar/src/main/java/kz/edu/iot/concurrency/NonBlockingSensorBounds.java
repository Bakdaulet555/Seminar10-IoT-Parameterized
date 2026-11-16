package kz.edu.iot.concurrency;

import java.util.concurrent.atomic.AtomicReference;

public class NonBlockingSensorBounds {

    public record Bounds(double min, double max) {
        public Bounds {
            if (min > max) {
                throw new IllegalArgumentException("min > max");
            }
        }
    }

    private final AtomicReference<Bounds> current =
            new AtomicReference<>(null);

    public void updateReading(double value) {
        if (!Double.isFinite(value)) {
            throw new IllegalArgumentException("Invalid sensor value");
        }

        while (true) {
            Bounds old = current.get();

            Bounds next = old == null
                    ? new Bounds(value, value)
                    : new Bounds(
                            Math.min(old.min(), value),
                            Math.max(old.max(), value)
                    );

            if (old != null && next.equals(old)) {
                return;
            }

            if (current.compareAndSet(old, next)) {
                return;
            }
        }
    }

    public Bounds getBounds() {
        return current.get();
    }
}
