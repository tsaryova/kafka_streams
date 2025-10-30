package com.example.kafkastreams.app.event;

import com.example.kafkastreams.domain.ClickEvent;
import com.example.kafkastreams.domain.OrderEvent;
import com.example.kafkastreams.domain.PaymentEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaEventProducer {
    private final KafkaTemplate<String, OrderEvent> orderKafkaTemplate;
    private final KafkaTemplate<String, PaymentEvent> paymentKafkaTemplate;
    private final KafkaTemplate<String, ClickEvent> clickKafkaTemplate;

    public KafkaEventProducer(
            KafkaTemplate<String, OrderEvent> orderKafkaTemplate,
            KafkaTemplate<String, PaymentEvent> paymentKafkaTemplate,
            KafkaTemplate<String, ClickEvent> clickKafkaTemplate
    ) {
        this.orderKafkaTemplate = orderKafkaTemplate;
        this.paymentKafkaTemplate = paymentKafkaTemplate;
        this.clickKafkaTemplate = clickKafkaTemplate;
    }

    public void sendOrder(String orderId, OrderEvent event) {
        orderKafkaTemplate.send("orders", orderId, event);
    }

    public void sendPayment(String orderId, PaymentEvent event) {
        paymentKafkaTemplate.send("payments", orderId, event);
    }

    public void sendClick(String userId, ClickEvent event) {
        clickKafkaTemplate.send("clicks", userId, event);
    }
}
