package org.acme.qkt.model;

public record DisplayOrderEvent(
    String orderId,
    String customerName
) {}
