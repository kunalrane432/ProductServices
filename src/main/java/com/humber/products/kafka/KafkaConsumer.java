package com.humber.products.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.humber.products.model.Product;
import com.humber.products.service.ProductService;

@Service
public class KafkaConsumer {

	@Autowired
    private  ProductService productService;

    @KafkaListener(topics = "order-topic", groupId = "product-group")
    public void consume(OrderPlacedEvent event) {
    	System.out.println("Consuming the orders placed event");
        event.getOrderItems().forEach(orderItem -> {
            Product product = productService.getProductById(orderItem.getProductId()).orElseThrow();
            product.setStock(product.getStock() - orderItem.getQuantity());
            productService.updateProduct(product);
        });
    }
}

