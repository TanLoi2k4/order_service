package com.tlcn.order_service.model;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.tlcn.order_service.enums.ShipmentStatus;
import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;

@Entity
@Table(name = "shipments")
@Getter 
@Setter 
@NoArgsConstructor 
@AllArgsConstructor 
@Builder
public class Shipment {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @JsonBackReference("subOrder-shipment")
    @OneToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "sub_order_id", nullable = false, unique = true)
    private SubOrder subOrder;

    @Column(name = "external_shipment_id", length = 120)
    private String externalShipmentId;

    @Column(name = "carrier_code", length = 80)
    private String carrierCode;

    @Column(name = "mocked")
    @Builder.Default
    private Boolean mocked = false;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    @Builder.Default
    private ShipmentStatus status = ShipmentStatus.CREATED;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at")
    private Instant updatedAt;

}