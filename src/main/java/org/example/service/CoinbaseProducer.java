package org.example.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.config.CoinbaseConfig;
import org.example.http.CoinbaseWebSocketListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

@Slf4j
@Service
@RequiredArgsConstructor
public class CoinbaseProducer {

    private final CoinbaseConfig coinbaseConfig;

    @Autowired
    private final BlockingQueue<String> queue;

    @EventListener(ApplicationReadyEvent.class)
    public void subscribe() {
        CountDownLatch latch = new CountDownLatch(1);

        String subscribeMessage = "{"
                + "\"type\": \"subscribe\","
                + "\"channel\": \""+coinbaseConfig.getChannel()+"\","
                + "\"product_ids\": [\""+coinbaseConfig.getProductIds()+"\"]"
                + "}";
        log.info("Subscribe message: {}", subscribeMessage);
        CoinbaseWebSocketListener listener = new CoinbaseWebSocketListener(latch, subscribeMessage, queue);

        log.info("Connecting to Coinbase Advanced Trade WebSocket...");
        HttpClient.newHttpClient()
                .newWebSocketBuilder()
                .buildAsync(URI.create(coinbaseConfig.getEndpoint()), listener)
                .join();

        try {
            latch.await();
        } catch (Exception e) {
            log.error("error", e);
        }
    }

}
