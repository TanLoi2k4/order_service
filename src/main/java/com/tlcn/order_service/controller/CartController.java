package com.tlcn.order_service.controller;

import com.tlcn.order_service.model.Cart;
import com.tlcn.order_service.model.CartItem;
import com.tlcn.order_service.dto.OrderResponseDTO;
import com.tlcn.order_service.dto.ResponseDTO;
import com.tlcn.order_service.service.CartService;
import com.tlcn.order_service.service.OrderService; 
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import jakarta.validation.Valid;
import java.util.Map;

@RestController
@RequestMapping("/api/cart")
@RequiredArgsConstructor
public class CartController {
    private final CartService cartService;
    
    // =========================================================================
    // GET /api/cart
    // =========================================================================

    @GetMapping
    @PreAuthorize("hasRole('CUSTOMER')")
    public ResponseEntity<ResponseDTO<Cart>> getCart(Authentication authentication) {
        Long customerId = cartService.getCustomerIdFromKeycloakId(authentication.getName());
        Cart cart = cartService.getCart(customerId);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Cart retrieved successfully", cart));
    }
    
    // =========================================================================
    // POST /api/cart/items (Thêm/Tăng số lượng)
    // =========================================================================
    
    @PostMapping("/items")
    @PreAuthorize("hasRole('CUSTOMER')")
    public ResponseEntity<ResponseDTO<Cart>> addItemToCart(
            @Valid @RequestBody Map<String, Object> request,
            Authentication authentication) {
        
        // 1. Lấy thông tin từ Request
        Long productId = ((Number) request.get("productId")).longValue();
        int quantity = (Integer) request.get("quantity");
        
        // 2. Lấy Customer ID
        Long customerId = cartService.getCustomerIdFromKeycloakId(authentication.getName());
        
        // 3. Tạo CartItem (Lưu ý: Price và Vendor ID sẽ được lấy trong Service)
        // Chúng ta chỉ cần productId và quantity từ request
        CartItem newItem = new CartItem();
        newItem.setProductId(productId);
        
        Cart cart = cartService.addItemToCart(customerId, newItem, quantity);
        
        return ResponseEntity.ok(new ResponseDTO<>(true, "Item added to cart successfully", cart));
    }
    
    // =========================================================================
    // PATCH /api/cart/items/{itemId} (Cập nhật số lượng)
    // =========================================================================
    
    @PatchMapping("/items/{itemId}")
    @PreAuthorize("hasRole('CUSTOMER')")
    public ResponseEntity<ResponseDTO<Cart>> updateItemQuantity(
            @PathVariable String itemId,
            @RequestBody Map<String, Integer> updateRequest,
            Authentication authentication) {
        
        if (updateRequest.get("quantity") == null) {
            throw new OrderService.CustomException("Quantity field is required");
        }
        int newQuantity = updateRequest.get("quantity");
        
        Long customerId = cartService.getCustomerIdFromKeycloakId(authentication.getName());
        Cart cart = cartService.updateItemQuantity(customerId, itemId, newQuantity);
        
        return ResponseEntity.ok(new ResponseDTO<>(true, "Item quantity updated successfully", cart));
    }

    // =========================================================================
    // DELETE /api/cart/items/{itemId}
    // =========================================================================
    
    @DeleteMapping("/items/{itemId}")
    @PreAuthorize("hasRole('CUSTOMER')")
    public ResponseEntity<ResponseDTO<Cart>> removeItemFromCart(
            @PathVariable String itemId,
            Authentication authentication) {
        Long customerId = cartService.getCustomerIdFromKeycloakId(authentication.getName());
        Cart cart = cartService.removeItemFromCart(customerId, itemId);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Item removed from cart successfully", cart));
    }
    
    // =========================================================================
    // POST /api/cart/checkout (Chuyển sang Order)
    // =========================================================================

    @PostMapping("/checkout")
    @PreAuthorize("hasRole('CUSTOMER')")
    public ResponseEntity<ResponseDTO<OrderResponseDTO>> checkoutCart(
            @RequestBody Map<String, String> checkoutRequest, 
            Authentication authentication) {
        
        String keycloakId = authentication.getName();
        String shippingAddress = checkoutRequest.get("shippingAddress");
        
        if (shippingAddress == null || shippingAddress.trim().isEmpty()) {
             throw new OrderService.CustomException("Shipping address is required for checkout");
        }
        
        Long customerId = cartService.getCustomerIdFromKeycloakId(keycloakId);
        OrderResponseDTO response = cartService.checkoutCart(customerId, shippingAddress, keycloakId);
        
        return ResponseEntity.ok(new ResponseDTO<>(true, "Checkout successful. Order created", response));
    }
}