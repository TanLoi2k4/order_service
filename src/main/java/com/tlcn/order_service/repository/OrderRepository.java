package com.tlcn.order_service.repository;

import com.tlcn.order_service.model.Order;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.time.Instant;
import java.util.List;

@Repository
public interface OrderRepository extends JpaRepository<Order, Long>, JpaSpecificationExecutor<Order> {

    Optional<Order> findByOrderCode(String orderCode);

    Optional<Order> findByOrderCodeAndCustomerKeycloakId(String orderCode, String customerKeycloakId);

    Page<Order> findByCustomerKeycloakId(String customerKeycloakId, Pageable pageable);

    List<Order> findByCustomerKeycloakId(String customerKeycloakId);

    List<Order> findByPaymentStatusAndCreatedAtBefore(com.tlcn.order_service.enums.PaymentStatus paymentStatus, Instant createdAtBefore);

    List<Order> findByCreatedAtGreaterThanEqualAndCreatedAtLessThan(Instant from, Instant to);
}
