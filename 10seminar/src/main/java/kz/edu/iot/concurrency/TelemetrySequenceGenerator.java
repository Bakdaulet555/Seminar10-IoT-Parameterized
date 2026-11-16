package kz.edu.iot.concurrency;

import java.util.concurrent.atomic.AtomicLong;

public final class TelemetrySequenceGenerator {
    private final AtomicLong sequenceId = new AtomicLong(0);
    private final String gatewayNodeId;

    public TelemetrySequenceGenerator(String gatewayNodeId) {
        if (gatewayNodeId == null || gatewayNodeId.isBlank()) {
            throw new IllegalArgumentException("Gateway ID cannot be empty");
        }
        this.gatewayNodeId = gatewayNodeId;
    }

    public String nextPacketId() {
        long id = sequenceId.incrementAndGet();
        return gatewayNodeId + "-" + id;
    }

    public synchronized String nextPacketIdSynchronized() {
        long id = sequenceId.incrementAndGet();
        return gatewayNodeId + "-" + id;
    }

    public long getCurrentCount() {
        return sequenceId.get();
    }
}
