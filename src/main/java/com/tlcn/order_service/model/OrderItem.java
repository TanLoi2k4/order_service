package com.tlcn.order_service.model;

import com.fasterxml.jackson.annotation.JsonBackReference;
import jakarta.persistence.*;
import lombok.*;
import java.math.BigDecimal;

import com.tlcn.order_service.enums.OrderItemStatus;

@Entity
@Table(
    name = "order_items",
    indexes = {
        @Index(name = "idx_order_items_product_id", columnList = "product_id"),
        @Index(name = "idx_order_items_sub_order_product", columnList = "sub_order_id, product_id")
    }
)
@Getter 
@Setter 
@NoArgsConstructor 
@AllArgsConstructor 
@Builder
public class OrderItem {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JsonBackReference("subOrder-items")
    @JoinColumn(name = "sub_order_id", nullable = false)
    private SubOrder subOrder;

    @Enumerated(EnumType.STRING)
    @Builder.Default
    private OrderItemStatus status = OrderItemStatus.PENDING;
    
    private Long productId;
    private Integer quantity;

    @Column(precision = 19, scale = 2)
    private BigDecimal unitPrice; 

    @Column(precision = 19, scale = 2)
    private BigDecimal lineTotal; 
}