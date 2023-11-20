package org.acme.qkt.model;

public record OrderEvent(
    String orderId,
    OrderEventType eventType,
    String customerId
) {
    public enum OrderEventType {
        UPDATED, // order first created or subsequently updated
        RAISED   // order submitted for processing
    }
}
