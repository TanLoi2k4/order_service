package com.tlcn.order_service.repository;

import com.tlcn.order_service.model.Shipment;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface ShipmentRepository extends JpaRepository<Shipment, Long> {

    Optional<Shipment> findBySubOrder_Id(Long subOrderId);
}
