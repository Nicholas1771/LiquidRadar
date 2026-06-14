package org.example.http;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.net.http.WebSocket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;

@Slf4j
@RequiredArgsConstructor
public class CoinbaseWebSocketListener implements WebSocket.Listener {

    private final StringBuilder buffer = new StringBuilder();
    private final CountDownLatch latch;
    private final String subscribeMessage;
    private final BlockingQueue<String> queue;

    @Override
    public void onOpen(WebSocket webSocket) {
        log.info("WebSocket connection fully open. Sending subscription payload...");

        webSocket.sendText(subscribeMessage, true);
        webSocket.request(1);
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        buffer.append(data);
        if (last) {
            log.info("RECEIVED: [{}]", buffer);
            try {
                queue.put(buffer.toString());
            } catch (InterruptedException e) {
                log.error("InterruptedException while adding to string to queue, string {}, error:", buffer, e);
            }
            buffer.setLength(0);
        }
        webSocket.request(1);
        log.info("In queue {}", queue.size());
        return null;
    }

    @Override
    public void onError(WebSocket webSocket, Throwable e) {
        log.error("WebSocket Error: ", e);
        latch.countDown();
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        log.info("Connection closed: {}", reason);
        latch.countDown();
        return null;
    }
}
