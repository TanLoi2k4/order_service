package com.tlcn.order_service.repository;

import com.tlcn.order_service.model.OrderItem;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderItemRepository extends JpaRepository<OrderItem, Long> {
    Page<OrderItem> findByVendorKeycloakId(String vendorKeycloakId, Pageable pageable);
}