package com.tlcn.order_service.repository;

import com.tlcn.order_service.model.Cart;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface CartRepository extends JpaRepository<Cart, Long> {
    Optional<Cart> findByKeycloakId(String keycloakId);
}