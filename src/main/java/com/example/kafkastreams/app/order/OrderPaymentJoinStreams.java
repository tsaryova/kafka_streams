package com.example.kafkastreams.app.order;

import com.example.kafkastreams.domain.OrderEvent;
import com.example.kafkastreams.domain.PaymentEvent;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class OrderPaymentJoinStreams {
    public OrderPaymentJoinStreams(
            StreamsBuilder builder,
            Serde<OrderEvent> orderEventSerde,
            Serde<PaymentEvent> paymentEventSerde
    ) {
        KStream<String, OrderEvent> orders = builder.stream("orders", Consumed.with(Serdes.String(), orderEventSerde));
        KStream<String, PaymentEvent> payments = builder.stream("payments", Consumed.with(Serdes.String(), paymentEventSerde));

        // Джойним по orderId (ключ должен совпадать!)
        // Предполагаем, что ключ — orderId
        ValueJoiner<OrderEvent, PaymentEvent, String> joiner = (order, payment) ->
                "Order " + order.getOrderId() + " is " + payment.getStatus();

        orders.peek((key, order) -> System.out.println("Order key=" + key + ", orderId=" + order.getOrderId()))
                .join(payments.peek((key, payment) -> System.out.println("Payment key=" + key + ", orderId=" + payment.getOrderId())),
                        joiner, JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)))
                .peek((key, value) -> System.out.println("Output to order-payment-status: " + value))
                .to("order-payment-status", Produced.with(Serdes.String(), Serdes.String()));
    }
}
