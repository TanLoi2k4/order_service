// File: com/tlcn/order_service/model/CartItem.java

package com.tlcn.order_service.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.UUID;

@Data 
@NoArgsConstructor
public class CartItem implements Serializable {
    
    private String itemId = UUID.randomUUID().toString();
    private Long productId;
    private Long vendorId; 
    private Integer quantity;
    private BigDecimal price;
    private String productName;

    public CartItem(Long productId, Long vendorId, Integer quantity, BigDecimal price, String productName) {
        this.productId = productId;
        this.vendorId = vendorId;
        this.quantity = quantity;
        this.price = price;
        this.productName = productName;
    }
}