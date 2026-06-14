package org.example.map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.example.model.Trade;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class TradeFlatMap {

    private final ObjectMapper objectMapper;

    public TradeFlatMap() {
        objectMapper = new ObjectMapper();
    }

    public List<Trade> map (String trades) {
        try {
            JsonNode root = objectMapper.readTree(trades);
            JsonNode events = root.path("events");
            if (!events.isArray()) return new ArrayList<>();

            List<Trade> tradeList = new ArrayList<>();

            for (JsonNode event : events) {
                String eventType = event.path("type").asText();
                JsonNode tradesNode = event.path("trades");
                if (!tradesNode.isArray()) continue;

                for (JsonNode tradeNode : tradesNode) {
                    Trade trade = Trade.builder()
                            .eventType(eventType)
                            .tradeId(tradeNode.path("trade_id").asText())
                            .price(tradeNode.path("price").asDouble())
                            .size(tradeNode.path("size").asDouble())
                            .time(Instant.parse(tradeNode.path("time").asText()).toEpochMilli())
                            .side(tradeNode.path("side").asText())
                            .build();
                    tradeList.add(trade);
                }
            }
            return tradeList;
        } catch (JsonProcessingException e) {
            log.error("Error mapping trades message [{}]:", trades, e);
            return new ArrayList<>();
        }
    }

}
