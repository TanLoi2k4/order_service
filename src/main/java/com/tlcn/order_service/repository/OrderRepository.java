package com.tlcn.order_service.repository;

import com.tlcn.order_service.model.Order;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order, Long> {
    Page<Order> findByKeycloakId(String keycloakId, Pageable pageable);
}