package com.tlcn.order_service.model;

import lombok.Data;
import java.math.BigDecimal;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
public class Cart implements Serializable {
    private Long customerId; 
    private List<CartItem> items = new ArrayList<>(); 
    private BigDecimal totalAmount = BigDecimal.ZERO;
    private Integer totalItemsQuantity = 0;
    private Long lastUpdatedAt = System.currentTimeMillis();

    public Cart(Long customerId) {
        this.customerId = customerId;
    }
    
    public void recalculate() {
        if (items == null || items.isEmpty()) {
            this.totalAmount = BigDecimal.ZERO;
            this.totalItemsQuantity = 0;
            return;
        }
        
        this.totalAmount = items.stream()
            .map(item -> item.getPrice().multiply(BigDecimal.valueOf(item.getQuantity())))
            .reduce(BigDecimal.ZERO, BigDecimal::add);

        this.totalItemsQuantity = items.stream()
            .mapToInt(CartItem::getQuantity)
            .sum();
            
        this.lastUpdatedAt = System.currentTimeMillis();
    }
}