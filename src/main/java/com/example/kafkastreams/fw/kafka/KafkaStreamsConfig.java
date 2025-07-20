package com.example.kafkastreams.fw.kafka;

import com.example.kafkastreams.domain.Purchase;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.streams.StreamsConfig.*;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    @Bean
    public KStream<String, Purchase> kStream(StreamsBuilder streamsBuilder) {
        // Десериализация JSON → Purchase
        JsonSerde<Purchase> purchaseSerde = new JsonSerde<>(Purchase.class);
        KStream<String, Purchase> stream = streamsBuilder.stream(
                "purchases-topic",
                Consumed.with(Serdes.String(), purchaseSerde)
        );

        // 1. Маскировка email и отправка в masked-purchases
        KStream<String, Purchase> maskedStream = stream.mapValues(purchase ->
                new Purchase(
                        purchase.orderId(),
                        purchase.customerName(),
                        "***masked***",  // Маскируем email
                        purchase.product(),
                        purchase.price(),
                        purchase.storeType()
                )
        );
        maskedStream.to("masked-purchases", Produced.with(Serdes.String(), purchaseSerde));

        // 2. Фильтрация по типу магазина
        stream.split()
                .branch(
                        (key, purchase) -> "electronics".equals(purchase.storeType()),
                        Branched.withConsumer(ks -> ks.to("electronics-purchases"))
                )
                .branch(
                        (key, purchase) -> "coffee".equals(purchase.storeType()),
                        Branched.withConsumer(ks -> ks.to("coffee-purchases"))
                );

        // 3. Агрегация популярных товаров (KTable)
        KTable<String, Long> popularProducts = stream
                .groupBy((key, purchase) -> purchase.product())
                .count(Materialized.as("popular-products-store"));
        popularProducts.toStream().to("popular-products");

        return stream;
    }
}
