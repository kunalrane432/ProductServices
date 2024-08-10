package com.humber.products.kafka;
import java.util.Map;

public class OrderPlacedEvent {
    private String orderId;
    private Map<Long, Integer> products; 

    // Constructors
    public OrderPlacedEvent() {}

    public OrderPlacedEvent(String orderId, Map<Long, Integer> products) {
        this.orderId = orderId;
        this.products = products;
    }

    // Getters and Setters
    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Map<Long, Integer> getProducts() {
        return products;
    }

    public void setProducts(Map<Long, Integer> products) {
        this.products = products;
    }

    @Override
    public String toString() {
        return "OrderPlacedEvent{" +
                "orderId='" + orderId + '\'' +
                ", products=" + products +
                '}';
    }
}


