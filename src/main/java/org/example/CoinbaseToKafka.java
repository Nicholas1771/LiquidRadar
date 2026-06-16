package org.example;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@Slf4j
@ConfigurationPropertiesScan
@SpringBootApplication
@EnableScheduling
public class CoinbaseToKafka implements CommandLineRunner {

    @Override
    public void run(String... args) {
        log.info("Starting app");
    }

    public static void main(String[] args) {
        SpringApplication.run(CoinbaseToKafka.class, args);
    }
}
