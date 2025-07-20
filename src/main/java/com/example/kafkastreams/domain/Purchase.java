package com.example.kafkastreams.domain;

public record Purchase(
        String orderId,
        String customerName,
        String email,       // Будем маскировать
        String product,
        double price,
        String storeType    // "electronics" или "coffee"
) {}
