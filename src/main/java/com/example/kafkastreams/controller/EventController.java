package com.example.kafkastreams.controller;

import com.example.kafkastreams.app.event.KafkaEventProducer;
import com.example.kafkastreams.domain.ClickEvent;
import com.example.kafkastreams.domain.OrderEvent;
import com.example.kafkastreams.domain.PaymentEvent;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class EventController {
    private final KafkaEventProducer kafkaEventProducer;

    public EventController(KafkaEventProducer kafkaEventProducer) {
        this.kafkaEventProducer = kafkaEventProducer;
    }

    @PostMapping("/orders")
    public ResponseEntity<String> createOrder(@RequestBody OrderEvent order) {
        kafkaEventProducer.sendOrder(order.getOrderId(), order);
        return ResponseEntity.ok("Order " + order.getOrderId() + " sent to Kafka");
    }

    @PostMapping("/payments")
    public ResponseEntity<String> createPayment(@RequestBody PaymentEvent payment) {
        kafkaEventProducer.sendPayment(payment.getOrderId(), payment);
        return ResponseEntity.ok("Payment for order " + payment.getStatus() + " sent to Kafka");
    }

    @PostMapping("/clicks")
    public ResponseEntity<String> trackClick(@RequestBody ClickEvent click) {
        kafkaEventProducer.sendClick(click.getUserId(), click);
        return ResponseEntity.ok("Click by user " + click.getUserId() + " sent to Kafka");
    }
}
