package org.example.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.BlockingQueue;

@Slf4j
@Component
@RequiredArgsConstructor
public class CoinbaseMessageHandler {

    private final BlockingQueue<String> queue;
    private static final int QUEUE_WARN_THRESHOLD = 10;

    private volatile Instant lastHeartbeat = Instant.now();

    public void handle(String message) {
        log.info("Got message {}", message);
        if (message.contains("\"channel\":\"heartbeats\"")) {
            lastHeartbeat = Instant.now();
            log.debug("Heartbeat received");
            return;
        }

        log.info("Received message from Coinbase");
        try {
            queue.put(message);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while adding message to queue: {}", message, e);
        }
        if (queue.size() > QUEUE_WARN_THRESHOLD) {
            log.warn("Queue size {} exceeds threshold", queue.size());
        }
    }

    public boolean isStale(Duration threshold) {
        return Duration.between(lastHeartbeat, Instant.now()).compareTo(threshold) > 0;
    }
}