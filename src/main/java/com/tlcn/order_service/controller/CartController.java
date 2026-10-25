package com.tlcn.order_service.controller;

import com.tlcn.order_service.dto.CartRequest;
import com.tlcn.order_service.dto.ResponseDTO;
import com.tlcn.order_service.model.Cart;
import com.tlcn.order_service.service.CartService;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/cart")
@Slf4j
public class CartController {

    @Autowired
    private CartService cartService;

    @GetMapping
    @PreAuthorize("hasRole('CUSTOMER')")
    public ResponseEntity<ResponseDTO<Cart>> getCart(Authentication authentication) {
        String keycloakId = authentication.getName();
        Cart cart = cartService.getCart(keycloakId);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Cart retrieved successfully", cart));
    }

    @PostMapping
    @PreAuthorize("hasRole('CUSTOMER')")
    public ResponseEntity<ResponseDTO<Cart>> addToCart(
            @Valid @RequestBody CartRequest request,
            Authentication authentication) {

        String keycloakId = authentication.getName();
        Cart cart = cartService.addToCart(request, keycloakId);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Item added to cart successfully", cart));
    }

    @DeleteMapping("/{productId}")
    @PreAuthorize("hasRole('CUSTOMER')")
    public ResponseEntity<ResponseDTO<Cart>> removeFromCart(
            @PathVariable Long productId,
            Authentication authentication) {

        String keycloakId = authentication.getName();
        Cart cart = cartService.removeFromCart(productId, keycloakId);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Item removed from cart successfully", cart));
    }

    @PutMapping
    @PreAuthorize("hasRole('CUSTOMER')")
    public ResponseEntity<ResponseDTO<Cart>> updateCartItem(
            @Valid @RequestBody CartRequest request,
            Authentication authentication) {

        String keycloakId = authentication.getName();
        Cart cart = cartService.updateCartItem(request, keycloakId);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Cart item updated successfully", cart));
    }
}