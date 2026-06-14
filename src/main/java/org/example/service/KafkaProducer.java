package org.example.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.config.Config;
import org.example.map.TradeFlatMap;
import org.example.model.Trade;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducer {

    private final BlockingQueue<String> queue;
    private final ExecutorService executor;
    private final Config.KafkaConfig kafkaConfig;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final TradeFlatMap tradeFlatMap;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostConstruct
    public void consume() {
        executor.submit(() -> {
            log.info("KafkaProducer thread started, waiting for data...");
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    String tradeData = queue.take();
                    List<Trade> trades = tradeFlatMap.map(tradeData);
                    if (trades == null || trades.isEmpty()) {
                        log.warn("No trades for kafka for message {}", tradeData);
                        continue;
                    }
                    log.info("Sending {} trades to kafka", trades.size());

                    for (Trade trade : trades) {
                        kafkaTemplate.send(kafkaConfig.getTopic(), trade.tradeId(), objectMapper.writeValueAsString(trade))
                            .whenComplete((result, ex) -> {
                                if (ex != null) {
                                    // This executes if Kafka rejects the message or times out
                                    log.error("ERROR sending record to Kafka topic {}", tradeData, ex);
                                } else {
                                    log.debug("Successfully delivered to topic {} partition {}",
                                            result.getRecordMetadata().topic(),
                                            result.getRecordMetadata().partition());
                                }
                            });
                    }
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
