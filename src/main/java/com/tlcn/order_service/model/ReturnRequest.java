package com.tlcn.order_service.model;

import com.tlcn.order_service.enums.ReturnReason;
import com.tlcn.order_service.enums.ReturnRequestStatus;
import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(
    name = "return_requests",
    indexes = {
        @Index(name = "idx_return_requests_sub_order", columnList = "sub_order_id"),
        @Index(name = "idx_return_requests_vendor_status", columnList = "vendor_keycloak_id, status"),
        @Index(name = "idx_return_requests_customer", columnList = "customer_keycloak_id"),
        @Index(name = "idx_return_requests_status_created", columnList = "status, created_at")
    }
)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ReturnRequest {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "sub_order_id", nullable = false)
    private SubOrder subOrder;

    @Column(name = "order_code", nullable = false, length = 50)
    private String orderCode;

    @Column(name = "customer_keycloak_id", nullable = false)
    private String customerKeycloakId;

    @Column(name = "vendor_keycloak_id", nullable = false)
    private String vendorKeycloakId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    @Builder.Default
    private ReturnRequestStatus status = ReturnRequestStatus.REQUESTED;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    private ReturnReason reason;

    @Column(length = 1000)
    private String description;

    @Column(name = "seller_note", length = 1000)
    private String sellerNote;

    @Column(name = "customer_ship_note", length = 1000)
    private String customerShipNote;

    @Column(name = "admin_note", length = 1000)
    private String adminNote;

    @Column(name = "refund_amount", nullable = false, precision = 19, scale = 2)
    private BigDecimal refundAmount;

    @Column(name = "currency", nullable = false, length = 10)
    @Builder.Default
    private String currency = "VND";

    @OneToMany(mappedBy = "returnRequest", cascade = CascadeType.ALL, orphanRemoval = true)
    @Builder.Default
    private List<ReturnRequestItem> items = new ArrayList<>();

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at")
    private Instant updatedAt;

    @Column(name = "approved_at")
    private Instant approvedAt;

    @Column(name = "shipped_at")
    private Instant shippedAt;

    @Column(name = "received_at")
    private Instant receivedAt;

    @Column(name = "refunded_at")
    private Instant refundedAt;

    @Column(name = "return_shipment_id")
    private Long returnShipmentId;

    @Column(name = "return_external_shipment_id", length = 120)
    private String returnExternalShipmentId;

    @Column(name = "return_carrier_code", length = 80)
    private String returnCarrierCode;
}
