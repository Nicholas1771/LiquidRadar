package org.example.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.config.AppConfig;
import org.example.model.Trade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class TradeKafkaSink extends RichSinkFunction<Trade> {

    private static final Logger LOG = LoggerFactory.getLogger(TradeKafkaSink.class);
    private final AppConfig.KafkaConfig config;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private transient KafkaProducer<String, String> producer;

    public TradeKafkaSink(AppConfig.KafkaConfig config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        producer = new KafkaProducer<>(props);
    }

    @Override
    public void invoke(Trade value, Context context) throws Exception {
        String payload = objectMapper.writeValueAsString(value);
        producer.send(new ProducerRecord<>(config.topic, value.tradeId(), payload), callback(value))
                .get();
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.flush();
            producer.close();
        }
    }

    private Callback callback(Trade trade) {
        return (metadata, exception) -> {
            if (exception != null) {
                LOG.error("Failed to publish trade {} to Kafka", trade.tradeId(), exception);
            }
        };
    }
}
