package org.example.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

@Configuration
public class QueueConfig {

    @Bean
    public BlockingQueue<String> queue() {
        return new LinkedBlockingQueue<>(100);
    }

    @Bean
    public ExecutorService executor() {
        return Executors.newSingleThreadExecutor();
    }
}
