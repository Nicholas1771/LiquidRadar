package org.example.config;

import lombok.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

@Configuration
public class Config {

    @Value
    @ConfigurationProperties(prefix = "coinbase")
    public static class CoinbaseConfig {
        String endpoint;
        String channel;
        List<String> productIds;
    }

    @Value
    @ConfigurationProperties(prefix = "kafka")
    public static class KafkaConfig {
        String topic;
    }

    @Bean
    public BlockingQueue<String> queue() {
        return new LinkedBlockingQueue<>(100);
    }

    @Bean
    public ExecutorService executor() {
        return Executors.newSingleThreadExecutor();
    }

}
