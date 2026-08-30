package org.example.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public final class AppConfig {

    public final CoinbaseConfig coinbase;
    public final KafkaConfig kafka;

    private AppConfig(CoinbaseConfig coinbase, KafkaConfig kafka) {
        this.coinbase = coinbase;
        this.kafka = kafka;
    }

    public static AppConfig load() {
        Properties properties = new Properties();
        try (InputStream input = AppConfig.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input != null) {
                properties.load(input);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load application.properties", e);
        }

        CoinbaseConfig coinbase = new CoinbaseConfig(
                resolve("COINBASE_ENDPOINT", properties.getProperty("coinbase.endpoint"), "wss://advanced-trade-ws.coinbase.com"),
                resolve("COINBASE_CHANNEL", properties.getProperty("coinbase.channel"), "market_trades"),
                parseList(resolve("COINBASE_PRODUCT_IDS", properties.getProperty("coinbase.product_ids"), "BTC-USD"))
        );

        KafkaConfig kafka = new KafkaConfig(
                resolve("KAFKA_BOOTSTRAP_SERVERS", properties.getProperty("kafka.bootstrap-servers"), "localhost:9092"),
                resolve("KAFKA_TOPIC", properties.getProperty("kafka.topic"), "coinbase.trade.btcusd.raw")
        );

        return new AppConfig(coinbase, kafka);
    }

    private static String resolve(String envKey, String propertyValue, String defaultValue) {
        String envValue = System.getProperty(envKey);
        if (envValue == null || envValue.isBlank()) {
            envValue = System.getenv(envKey);
        }
        if (envValue != null && !envValue.isBlank()) {
            return envValue;
        }
        if (propertyValue != null && !propertyValue.isBlank()) {
            return propertyValue;
        }
        return defaultValue;
    }

    private static List<String> parseList(String value) {
        return Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(item -> !item.isEmpty())
                .toList();
    }

    public static final class CoinbaseConfig implements java.io.Serializable {
        public final String endpoint;
        public final String channel;
        public final List<String> productIds;

        private CoinbaseConfig(String endpoint, String channel, List<String> productIds) {
            this.endpoint = endpoint;
            this.channel = channel;
            this.productIds = productIds;
        }
    }

    public static final class KafkaConfig implements java.io.Serializable {
        public final String bootstrapServers;
        public final String topic;

        private KafkaConfig(String bootstrapServers, String topic) {
            this.bootstrapServers = bootstrapServers;
            this.topic = topic;
        }
    }
}
