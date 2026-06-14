package org.example;

import org.junit.jupiter.api.Test;

import static org.springframework.test.util.AssertionErrors.assertNotNull;

public class CoinbaseToS3Test {

    @Test
    public void appHasAGreeting() {
        CoinbaseToS3 classUnderTest = new CoinbaseToS3();
        assertNotNull("app should create", classUnderTest);
    }
}
