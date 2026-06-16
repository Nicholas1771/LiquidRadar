package org.example.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.config.Config;
import org.example.http.CoinbaseWebSocketListener;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

@Slf4j
@Service
@RequiredArgsConstructor
public class CoinbaseWebSocketClient {

    private final Config.CoinbaseConfig coinbaseConfig;
    private final CoinbaseMessageHandler messageHandler;
    private final ObjectMapper objectMapper;

    private volatile CoinbaseWebSocketListener listener;

    @EventListener(ApplicationReadyEvent.class)
    public void connect() {
        String subscribeMessage = buildSubscribeMessage(coinbaseConfig.getChannel());
        String heartbeatMessage = buildSubscribeMessage("heartbeats");
        log.info("Subscribe message: {}", subscribeMessage);

        int attempt = 0;
        while (!Thread.currentThread().isInterrupted()) {
            try {
                long backoff = Math.min(1000L * (1L << attempt), 30_000L);
                if (attempt > 0) {
                    log.info("Reconnecting in {}ms (attempt {})", backoff, attempt);
                    Thread.sleep(backoff);
                }

                CountDownLatch latch = new CountDownLatch(1);
                listener = new CoinbaseWebSocketListener(subscribeMessage, heartbeatMessage, messageHandler::handle, latch::countDown);

                log.info("Connecting to Coinbase WebSocket...");
                HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(10))
                        .build()
                        .newWebSocketBuilder()
                        .buildAsync(URI.create(coinbaseConfig.getEndpoint()), listener)
                        .orTimeout(15, java.util.concurrent.TimeUnit.SECONDS)
                        .join();

                latch.await();
                attempt++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @PreDestroy
    public void shutdown() {
        if (listener == null) return;
        WebSocket ws = listener.getWebSocket();
        if (ws != null && !ws.isOutputClosed()) {
            log.info("Closing WebSocket connection...");
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "shutdown").join();
        }
    }

    @Scheduled(fixedDelay = 5000)
    public void checkHeartbeat() {
        if (messageHandler.isStale(Duration.ofSeconds(10))) {
            log.warn("No heartbeat received in 10s — connection may be dead");
            // optionally force reconnect by closing the socket:
            shutdown();
        }
    }

    private String buildSubscribeMessage(String channel) {
        try {
            return objectMapper.writeValueAsString(Map.of(
                    "type", "subscribe",
                    "channel", channel,
                    "product_ids", coinbaseConfig.getProductIds()
            ));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to build subscribe message for channel: " + channel, e);
        }
    }
}