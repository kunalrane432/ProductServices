package com.humber.products.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import com.humber.products.model.Product;

public interface ProductRepository extends JpaRepository<Product, Long> {
    List<Product> findByNameContaining(String name);
}

