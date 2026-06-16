package org.example.http;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.net.http.WebSocket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

@Slf4j
@RequiredArgsConstructor
public class CoinbaseWebSocketListener implements WebSocket.Listener {

    private final String subscribeMessage;
    private final String heartbeatSubscribeMessage;
    private final Consumer<String> onMessage;
    private final Runnable onDisconnect;

    private final StringBuilder buffer = new StringBuilder();
    @Getter
    private WebSocket webSocket;

    @Override
    public void onOpen(WebSocket webSocket) {
        this.webSocket = webSocket;
        log.info("WebSocket open. Sending subscription payload...");
        webSocket.sendText(subscribeMessage, true)
                .thenRun(() -> webSocket.sendText(heartbeatSubscribeMessage, true));;
        webSocket.request(1);
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        buffer.append(data);
        if (last) {
            onMessage.accept(buffer.toString());
            buffer.setLength(0);
        }
        webSocket.request(1);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void onError(WebSocket webSocket, Throwable e) {
        log.error("WebSocket error: ", e);
        onDisconnect.run();
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        log.info("WebSocket closed: {}", reason);
        onDisconnect.run();
        return CompletableFuture.completedFuture(null);
    }
}