package org.example;

import org.junit.jupiter.api.Test;

import static org.springframework.test.util.AssertionErrors.assertNotNull;

public class CoinbaseToKafkaTest {

    @Test
    public void appHasAGreeting() {
        CoinbaseToKafka classUnderTest = new CoinbaseToKafka();
        assertNotNull("app should create", classUnderTest);
    }
}
