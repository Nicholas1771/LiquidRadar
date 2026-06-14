package org.example;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@Slf4j
@ConfigurationPropertiesScan
@SpringBootApplication
public class CoinbaseToS3 implements CommandLineRunner {

    @Override
    public void run(String... args) {
        log.info("Starting app");
    }

    public static void main(String[] args) {
        SpringApplication.run(CoinbaseToS3.class, args);
    }
}
