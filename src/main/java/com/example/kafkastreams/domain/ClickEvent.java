package com.example.kafkastreams.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ClickEvent {
    private String userId;
    private String productId;
    private String category;
    private long timestamp;
}
