package com.tlcn.order_service.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonBackReference;

@Entity
@Table(name = "cart_items")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CartItem {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @JsonBackReference
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "cart_id", nullable = false)
    private Cart cart;

    @Column(name = "product_id", nullable = false)
    private Long productId;

    @Column(name = "vendor_keycloak_id", nullable = false)
    private String vendorKeycloakId;

    @Column(nullable = false)
    private Integer quantity;

    /** Đơn giá / sản phẩm (không nhân số lượng). Line total = price × quantity. */
    @Column(name = "price", nullable = false, precision = 19, scale = 2)
    private BigDecimal price;

    @Version
    private Long version;
}