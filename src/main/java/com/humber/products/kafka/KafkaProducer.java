package com.humber.products.kafka;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducer {

    private final KafkaTemplate<String, OrderPlacedEvent> kafkaTemplate;

    public KafkaProducer(KafkaTemplate<String, OrderPlacedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishOrderPlacedEvent(OrderPlacedEvent event) {
        kafkaTemplate.send("order-topic", event);
    }
}

