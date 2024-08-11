package com.humber.products.kafka;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.humber.products.model.Product;
import com.humber.products.repository.ProductRepository;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class KafkaConsumerService {

    private final ProductRepository productRepository;
    private final ObjectMapper objectMapper;

    public KafkaConsumerService(ProductRepository productRepository) {
        this.productRepository = productRepository;
        this.objectMapper = new ObjectMapper();
    }

    @KafkaListener(topics = "${spring.kafka.consumer.topic.order-placed}", groupId = "product-service")
    public void listen(String message) {
        try {
            OrderPlacedEvent event = objectMapper.readValue(message, OrderPlacedEvent.class);
            updateProductStock(event);
        } catch (Exception e) {
            System.err.printf("Failed to process message: %s%n", e.getMessage());
            e.printStackTrace();
        }
    }

    private void updateProductStock(OrderPlacedEvent event) {
        // Extract product IDs and quantities from the event
        Map<Long, Integer> products = event.getProducts();
        
        // Iterate over products and update stock
        for (Map.Entry<Long, Integer> entry : products.entrySet()) {
            Long productId = entry.getKey();
            int quantity = entry.getValue();
            
            Product product = productRepository.findById(productId).orElse(null);
            if (product != null) {
                // Deduct stock, assuming we want to decrease stock based on order quantity
                int newStock = product.getStock() - quantity;
                
                // Check if stock goes negative (handle as needed, e.g., log warning)
                if (newStock < 0) {
                    System.err.printf("Product %d has insufficient stock. Current stock: %d, Required: %d%n", productId, product.getStock(), quantity);
                    newStock = 0; // Or handle according to your business logic
                }

                product.setStock(newStock);
                productRepository.save(product);
                System.out.printf("Updated product %d stock to %d%n", productId, newStock);
            } else {
                System.err.printf("Product %d not found%n", productId);
            }
        }
    }
}


