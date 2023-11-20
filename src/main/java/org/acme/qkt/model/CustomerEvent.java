package org.acme.qkt.model;

public record CustomerEvent(
    String customerId,
    String customerName
) {}
