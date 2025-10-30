package com.example.kafkastreams.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ClickEvent {
    public String userId;
    public String productId;
    public String category;
    public long timestamp;
}
