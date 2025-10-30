package com.example.kafkastreams.fw.kafka;

import com.example.kafkastreams.app.serde.JsonSerde;
import com.example.kafkastreams.domain.ClickEvent;
import com.example.kafkastreams.domain.OrderEvent;
import com.example.kafkastreams.domain.PaymentEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaStreamsConfig {
    @Bean
    public KafkaTemplate<String, OrderEvent> orderKafkaTemplate(
            ProducerFactory<String, OrderEvent> orderProducerFactory
    ) {
        return new KafkaTemplate<>(orderProducerFactory);
    }

    @Bean
    public KafkaTemplate<String, PaymentEvent> paymentKafkaTemplate(
            ProducerFactory<String, PaymentEvent> paymentProducerFactory
    ) {
        return new KafkaTemplate<>(paymentProducerFactory);
    }

    @Bean
    public KafkaTemplate<String, ClickEvent> clickKafkaTemplate(
            ProducerFactory<String, ClickEvent> clickProducerFactory
    ) {
        return new KafkaTemplate<>(clickProducerFactory);
    }

    // ProducerFactory для OrderEvent
    @Bean
    public ProducerFactory<String, OrderEvent> orderProducerFactory() {
        return producerFactory(new JsonSerde<>(OrderEvent.class).serializer());
    }

    @Bean
    public ProducerFactory<String, PaymentEvent> paymentProducerFactory() {
        return producerFactory(new JsonSerde<>(PaymentEvent.class).serializer());
    }

    @Bean
    public ProducerFactory<String, ClickEvent> clickProducerFactory() {
        return producerFactory(new JsonSerde<>(ClickEvent.class).serializer());
    }

    private <T> ProducerFactory<String, T> producerFactory(Serializer<T> serializer) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer.getClass());
        return new DefaultKafkaProducerFactory<>(props);
    }
    @Bean
    public Serde<OrderEvent> orderEventSerde() {
        return new JsonSerde<>(OrderEvent.class);
    }

    @Bean
    public Serde<PaymentEvent> paymentEventSerde() {
        return new JsonSerde<>(PaymentEvent.class);
    }

    @Bean
    public Serde<ClickEvent> clickEventSerde() {
        return new JsonSerde<>(ClickEvent.class);
    }
}
