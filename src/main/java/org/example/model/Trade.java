package org.example.model;

import lombok.Builder;

@Builder
public record Trade (
        String eventType,
        String tradeId,
        Double price,
        Double size,
        Long time,
        String side
) {}