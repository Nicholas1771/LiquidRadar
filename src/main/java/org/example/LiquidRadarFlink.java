package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.config.AppConfig;
import org.example.map.TradeFlatMap;
import org.example.sink.TradeKafkaSink;
import org.example.source.CoinbaseWebSocketSource;

public class LiquidRadarFlink {

    public static void main(String[] args) throws Exception {
        AppConfig config = AppConfig.load();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new CoinbaseWebSocketSource(config.coinbase))
                .name("coinbase-websocket-source")
                .flatMap(new TradeFlatMap())
                .name("coinbase-trade-parser")
                .addSink(new TradeKafkaSink(config.kafka));

        env.execute("LiquidRadar Coinbase Stream");
    }
}
