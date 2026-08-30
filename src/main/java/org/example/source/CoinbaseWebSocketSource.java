package org.example.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.example.config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class CoinbaseWebSocketSource extends RichSourceFunction<String> {

    private static final Logger LOG = LoggerFactory.getLogger(CoinbaseWebSocketSource.class);
    private static final com.fasterxml.jackson.databind.ObjectMapper OBJECT_MAPPER = new com.fasterxml.jackson.databind.ObjectMapper();

    private final AppConfig.CoinbaseConfig config;
    private transient volatile boolean running;
    private transient WebSocket currentWebSocket;

    public CoinbaseWebSocketSource(AppConfig.CoinbaseConfig config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) {
        running = true;
    }

    @Override
    public void run(SourceFunction.SourceContext<String> ctx) throws Exception {
        int attempt = 0;

        while (running && !Thread.currentThread().isInterrupted()) {
            CountDownLatch disconnectLatch = new CountDownLatch(1);
            AtomicReference<Instant> lastHeartbeat = new AtomicReference<>(Instant.now());
            String subscribeMessage = buildSubscribeMessage(config.channel);
            String heartbeatMessage = buildSubscribeMessage("heartbeats");
            Object checkpointLock = ctx.getCheckpointLock();

            CoinbaseListener listener = new CoinbaseListener(
                    subscribeMessage,
                    heartbeatMessage,
                    message -> {
                        if (isHeartbeat(message)) {
                            lastHeartbeat.set(Instant.now());
                            return;
                        }

                        synchronized (checkpointLock) {
                            ctx.collect(message);
                        }
                    },
                    disconnectLatch::countDown
            );

            try {
                long backoff = Math.min(1000L * (1L << Math.min(attempt, 5)), 30_000L);
                if (attempt > 0) {
                    LOG.info("Reconnecting to Coinbase in {} ms (attempt {})", backoff, attempt);
                    Thread.sleep(backoff);
                }

                LOG.info("Connecting to Coinbase websocket endpoint {}", config.endpoint);
                currentWebSocket = HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(10))
                        .build()
                        .newWebSocketBuilder()
                        .buildAsync(URI.create(config.endpoint), listener)
                        .orTimeout(15, java.util.concurrent.TimeUnit.SECONDS)
                        .join();

                while (running && disconnectLatch.getCount() > 0) {
                    if (Duration.between(lastHeartbeat.get(), Instant.now()).compareTo(Duration.ofSeconds(10)) > 0) {
                        LOG.warn("Heartbeat stale; closing Coinbase websocket to reconnect");
                        closeCurrentWebSocket();
                        break;
                    }
                    Thread.sleep(5000);
                }

                attempt++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                LOG.error("Coinbase websocket source error", e);
                attempt++;
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
        closeCurrentWebSocket();
    }

    @Override
    public void close() {
        running = false;
        closeCurrentWebSocket();
    }

    private void closeCurrentWebSocket() {
        WebSocket webSocket = currentWebSocket;
        if (webSocket != null && !webSocket.isOutputClosed()) {
            try {
                webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "shutdown").join();
            } catch (Exception e) {
                LOG.warn("Failed to close Coinbase websocket cleanly", e);
            }
        }
    }

    private String buildSubscribeMessage(String channel) {
        try {
            return OBJECT_MAPPER.writeValueAsString(Map.of(
                    "type", "subscribe",
                    "channel", channel,
                    "product_ids", config.productIds
            ));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to build Coinbase subscription payload", e);
        }
    }

    private boolean isHeartbeat(String message) {
        return message.contains("\"channel\":\"heartbeats\"");
    }

    private final class CoinbaseListener implements WebSocket.Listener {
        private final String subscribeMessage;
        private final String heartbeatSubscribeMessage;
        private final Consumer<String> onMessage;
        private final Runnable onDisconnect;
        private final StringBuilder buffer = new StringBuilder();

        private CoinbaseListener(String subscribeMessage, String heartbeatSubscribeMessage, Consumer<String> onMessage, Runnable onDisconnect) {
            this.subscribeMessage = subscribeMessage;
            this.heartbeatSubscribeMessage = heartbeatSubscribeMessage;
            this.onMessage = onMessage;
            this.onDisconnect = onDisconnect;
        }

        @Override
        public void onOpen(WebSocket webSocket) {
            LOG.info("WebSocket open; sending Coinbase subscriptions");
            webSocket.sendText(subscribeMessage, true)
                    .thenRun(() -> webSocket.sendText(heartbeatSubscribeMessage, true));
            webSocket.request(1);
        }

        @Override
        public java.util.concurrent.CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            buffer.append(data);
            if (last) {
                onMessage.accept(buffer.toString());
                buffer.setLength(0);
            }
            webSocket.request(1);
            return java.util.concurrent.CompletableFuture.completedFuture(null);
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            LOG.error("Coinbase websocket error", error);
            onDisconnect.run();
        }

        @Override
        public java.util.concurrent.CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            LOG.info("Coinbase websocket closed: {}", reason);
            onDisconnect.run();
            return java.util.concurrent.CompletableFuture.completedFuture(null);
        }
    }
}
