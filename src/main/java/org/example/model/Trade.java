package org.example.model;

public record Trade(
        String eventType,
        String tradeId,
        Double price,
        Double size,
        Long time,
        String side
) {}
