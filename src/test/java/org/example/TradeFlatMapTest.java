package org.example;

import org.apache.flink.util.Collector;
import org.example.map.TradeFlatMap;
import org.example.model.Trade;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TradeFlatMapTest {

    @Test
    void parsesCoinbaseTradeEvents() throws Exception {
        String payload = """
                {
                  "events": [
                    {
                      "type": "update",
                      "trades": [
                        {
                          "trade_id": "t1",
                          "price": "62000.12",
                          "size": "0.25",
                          "time": "2026-08-30T16:12:45.000Z",
                          "side": "BUY"
                        },
                        {
                          "trade_id": "t2",
                          "price": "62001.50",
                          "size": "0.10",
                          "time": "2026-08-30T16:12:46.000Z",
                          "side": "SELL"
                        }
                      ]
                    }
                  ]
                }
                """;

        TradeFlatMap flatMap = new TradeFlatMap();
        List<Trade> collected = new ArrayList<>();

        flatMap.flatMap(payload, new Collector<>() {
            @Override
            public void collect(Trade record) {
                collected.add(record);
            }

            @Override
            public void close() {
            }
        });

        assertEquals(2, collected.size());
        assertEquals("t1", collected.get(0).tradeId());
        assertEquals("update", collected.get(0).eventType());
        assertEquals(62000.12, collected.get(0).price());
        assertEquals("SELL", collected.get(1).side());
    }
}
