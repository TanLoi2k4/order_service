package com.tlcn.order_service.controller;

import com.tlcn.order_service.dto.CartDTO;
import com.tlcn.order_service.dto.ResponseDTO;
import com.tlcn.order_service.model.Cart;
import com.tlcn.order_service.service.CartService;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/cart")
public class CartController {

    @Autowired
    private CartService cartService;

    @PostMapping
    @PreAuthorize("hasRole('CUSTOMER')")
    public ResponseEntity<ResponseDTO<Cart>> addToCart(
            @Valid @RequestBody CartDTO cartDTO,
            Authentication authentication) {
        String keycloakId = authentication.getName();
        Cart cart = cartService.addToCart(cartDTO, keycloakId);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Item added to cart successfully", cart));
    }

    @GetMapping
    @PreAuthorize("hasRole('CUSTOMER')")
    public ResponseEntity<ResponseDTO<Cart>> getCart(Authentication authentication) {
        String keycloakId = authentication.getName();
        Cart cart = cartService.getCart(keycloakId);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Cart retrieved successfully", cart));
    }

    @PutMapping
    @PreAuthorize("hasRole('CUSTOMER')")
    public ResponseEntity<ResponseDTO<Cart>> updateCartItem(
            @Valid @RequestBody CartDTO cartDTO,
            Authentication authentication) {
        String keycloakId = authentication.getName();
        Cart cart = cartService.updateCartItem(keycloakId, cartDTO.getProductId(), cartDTO.getQuantity());
        return ResponseEntity.ok(new ResponseDTO<>(true, "Cart item updated successfully", cart));
    }

    @DeleteMapping
    @PreAuthorize("hasRole('CUSTOMER')")
    public ResponseEntity<ResponseDTO<Void>> removeCartItem(
            @RequestParam Long productId,
            Authentication authentication) {
        String keycloakId = authentication.getName();
        cartService.removeCartItem(keycloakId, productId);
        return ResponseEntity.ok(new ResponseDTO<>(true, "Cart item removed successfully", null));
    }
}