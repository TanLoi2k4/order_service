package com.tlcn.order_service.model;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.tlcn.order_service.enums.PayoutStatus;
import com.tlcn.order_service.enums.SubOrderStatus;
import jakarta.persistence.*;
import lombok.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "sub_orders")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SubOrder {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JsonBackReference("order-subOrders")
    @JoinColumn(name = "order_id", nullable = false)
    private Order order;

    @Column(name = "vendor_keycloak_id", nullable = false)
    private String vendorKeycloakId;

    @Enumerated(EnumType.STRING)
    @Builder.Default
    private SubOrderStatus status = SubOrderStatus.CREATED;

    private BigDecimal subtotal;
    private BigDecimal finalAmount;
    private BigDecimal platformDiscountShare;

    @Column(name = "commission_fee", precision = 19, scale = 2)
    private BigDecimal commissionFee;

    @Enumerated(EnumType.STRING)
    @Column(name = "payout_status", nullable = false)
    @Builder.Default
    private PayoutStatus payoutStatus = PayoutStatus.PENDING;

    @Column(name = "payout_ref", length = 120)
    private String payoutRef;

    @Column(name = "payout_at")
    private Instant payoutAt;

    @Column(name = "completed_at")
    private Instant completedAt;

    @JsonManagedReference("subOrder-items")
    @OneToMany(mappedBy = "subOrder", cascade = CascadeType.ALL, orphanRemoval = true)
    @Builder.Default
    private List<OrderItem> items = new ArrayList<>();
}
