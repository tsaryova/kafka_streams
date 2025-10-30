package com.example.kafkastreams.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PaymentEvent {
    public String orderId;
    public String status; // "PAID", "FAILED"
    public long timestamp;
}
