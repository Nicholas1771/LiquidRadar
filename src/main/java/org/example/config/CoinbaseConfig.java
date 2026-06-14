package org.example.config;

import lombok.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Value
@ConfigurationProperties(prefix = "app.coinbase")
public class CoinbaseConfig {

    String endpoint;
    String keyId;
    String secret;

    String channel;
    String productIds;

}
