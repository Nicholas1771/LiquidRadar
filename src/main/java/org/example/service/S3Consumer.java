package org.example.service;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

@Slf4j
@Service
@RequiredArgsConstructor
public class S3Consumer {

    @Autowired
    private final BlockingQueue<String> queue;
    private final ExecutorService executor;

    @PostConstruct
    public void consume() {
        executor.submit(() -> {
            log.info("S3Consumer thread started, waiting for data...");
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    String tradeData = queue.take();
                    log.info("loading to S3: {}", tradeData);
                }
            } catch (InterruptedException e) {
                log.info("Consumer thread interrupted, shutting down smoothly.");
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.error("Error in consumer loop", e);
            }
        });
    }

}
