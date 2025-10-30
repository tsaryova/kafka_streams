package com.example.kafkastreams.app.order;

import com.example.kafkastreams.domain.OrderEvent;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.stereotype.Component;

@Component
public class SalesAggregationStreams {
    public SalesAggregationStreams(StreamsBuilder builder, Serde<OrderEvent> orderEventSerde) {
        KStream<String, OrderEvent> orders = builder.stream("orders", Consumed.with(Serdes.String(), orderEventSerde));

        // Группируем по категории и суммируем продажи
        orders
                .groupBy((key, order) -> order.category, Grouped.with(Serdes.String(), orderEventSerde))
                .aggregate(
                        () -> 0.0,
                        (category, order, total) -> total + order.getAmount(),
                        Materialized.as("sales-by-category")
                )
                .toStream()
                .mapValues(total -> "Total sales in " + total + "$")
                .to("sales-by-category-output", Produced.with(Serdes.String(), Serdes.String()));
    }
}
