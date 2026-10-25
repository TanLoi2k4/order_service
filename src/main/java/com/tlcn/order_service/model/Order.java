package com.tlcn.order_service.model;

import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.tlcn.order_service.enums.PaymentMethod;
import com.tlcn.order_service.enums.PaymentStatus;
import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Entity
@Table(
    name = "orders",
    indexes = {
        @Index(name = "idx_orders_customer_payment", columnList = "customer_keycloak_id, payment_status"),
        @Index(name = "idx_orders_order_code", columnList = "order_code")
    }
)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "order_code", unique = true, updatable = false, length = 50)
    private String orderCode;

    @Column(name = "customer_keycloak_id", nullable = false)
    private String customerKeycloakId;

    @Column(name = "platform_discount_code", length = 50)
    private String platformDiscountCode;

    @Column(name = "platform_discount_amount", precision = 19, scale = 2)
    @Builder.Default
    private BigDecimal platformDiscountAmount = BigDecimal.ZERO;

    // Legacy column kept in DB schema (often NOT NULL). Keep a non-null value to avoid insert failures.
    @Column(name = "discount_amount", nullable = false, precision = 19, scale = 2)
    @Builder.Default
    private BigDecimal discountAmount = BigDecimal.ZERO;

    @Column(name = "platform_discount_hold_id")
    private Long platformDiscountHoldId;

    @Column(name = "currency", nullable = false, length = 10)
    @Builder.Default
    private String currency = "VND";

    // FinalAmount = SUM(subOrders.finalAmount) - platformDiscountAmount
    @Column(name = "total_payable_amount", precision = 19, scale = 2)
    private BigDecimal totalPayableAmount;

    @Enumerated(EnumType.STRING)
    @Column(name = "payment_status", nullable = false)
    @Builder.Default
    private PaymentStatus paymentStatus = PaymentStatus.PENDING;

    @Enumerated(EnumType.STRING)
    private PaymentMethod paymentMethod;

    private String shippingAddress;
    private String customerName;
    private String customerPhone;
    private String note;

    @JsonManagedReference("order-subOrders")
    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL, orphanRemoval = true)
    @Builder.Default
    private List<SubOrder> subOrders = new ArrayList<>();

    @CreationTimestamp
    private Instant createdAt;

    @UpdateTimestamp
    private Instant updatedAt;

    @Version
    private Long version;

    // Flag to prevent duplicate stock reservation Kafka sends (exactly-once delivery guarantee)
    @Column(name = "stock_reservation_sent", nullable = false)
    @Builder.Default
    private boolean stockReservationSent = false;

    @PrePersist
    protected void onCreate() {
        if (this.orderCode == null) {
            this.orderCode = "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        }

        if (this.currency == null || this.currency.isBlank()) {
            this.currency = "VND";
        }

        if (this.platformDiscountAmount == null) {
            this.platformDiscountAmount = BigDecimal.ZERO;
        }

        if (this.discountAmount == null) {
            this.discountAmount = BigDecimal.ZERO;
        }

        if (this.paymentStatus == null) {
            this.paymentStatus = PaymentStatus.PENDING;
        }
    }
}