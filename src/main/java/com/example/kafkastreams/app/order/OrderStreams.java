package com.example.kafkastreams.app.order;

import com.example.kafkastreams.domain.OrderEvent;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.stereotype.Component;

@Component
public class OrderStreams {
    public OrderStreams(StreamsBuilder builder, Serde<OrderEvent> orderEventSerde) {
        KStream<String, OrderEvent> orders = builder.stream("orders", Consumed.with(Serdes.String(), orderEventSerde));

        // Только дорогие заказы (> 1000)
        KStream<String, OrderEvent> expensiveOrders = orders
                .filter((key, order) -> order.amount > 1000);

        // Преобразуем: добавляем префикс к ID
        KStream<String, String> orderSummaries = expensiveOrders
                .mapValues(order -> "EXPENSIVE ORDER: " + order.orderId + " for $" + order.amount);

        orderSummaries.to("expensive-orders-summary", Produced.with(Serdes.String(), Serdes.String()));
    }
}
