package org.example.map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.example.model.Trade;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class TradeFlatMap implements FlatMapFunction<String, Trade> {

    private static final Logger LOG = LoggerFactory.getLogger(TradeFlatMap.class);
    private final ObjectMapper objectMapper;

    public TradeFlatMap() {
        objectMapper = new ObjectMapper();
    }

    @Override
    public void flatMap(String trades, Collector<Trade> out) {
        try {
            JsonNode root = objectMapper.readTree(trades);
            JsonNode events = root.path("events");
            if (!events.isArray()) {
                return;
            }

            for (JsonNode event : events) {
                String eventType = event.path("type").asText();
                JsonNode tradesNode = event.path("trades");
                if (!tradesNode.isArray()) continue;

                for (JsonNode tradeNode : tradesNode) {
                    Trade trade = new Trade(
                            eventType,
                            tradeNode.path("trade_id").asText(),
                            tradeNode.path("price").asDouble(),
                            tradeNode.path("size").asDouble(),
                            Instant.parse(tradeNode.path("time").asText()).toEpochMilli(),
                            tradeNode.path("side").asText()
                    );
                    out.collect(trade);
                }
            }
        } catch (JsonProcessingException e) {
            LOG.error("Error mapping trades message [{}]:", trades, e);
            throw new IllegalArgumentException("Invalid Coinbase message", e);
        }
    }

}
