package com.tlcn.order_service.service;

import com.tlcn.order_service.dto.CartDTO;
import com.tlcn.order_service.dto.ProductDTO;
import com.tlcn.order_service.dto.ProductResponseDTO;
import com.tlcn.order_service.model.Cart;
import com.tlcn.order_service.model.CartItem;
import com.tlcn.order_service.repository.CartItemRepository;
import com.tlcn.order_service.repository.CartRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

@Service
public class CartService {

    private static final Logger log = LoggerFactory.getLogger(CartService.class);

    @Autowired
    private CartRepository cartRepository;

    @Autowired
    private CartItemRepository cartItemRepository;

    @Autowired
    @LoadBalanced
    private RestTemplate restTemplate;

    public static class CustomException extends RuntimeException {
        public CustomException(String message) {
            super(message);
        }
    }

    private ProductDTO getProduct(Long productId) {
        try {
            ResponseEntity<ProductResponseDTO> response = restTemplate.getForEntity(
                "http://product-service/api/products/" + productId,
                ProductResponseDTO.class);
                
            ProductResponseDTO responseBody = response.getBody();
                
            if (response.getStatusCode().isError() || responseBody == null || !responseBody.isSuccess() || responseBody.getData() == null) {
                throw new CustomException("Failed to fetch product with ID: " + productId);
            }
            
            return responseBody.getData();
        } catch (HttpClientErrorException e) {
            throw new CustomException("Error fetching product with ID: " + productId + 
                " (HTTP " + e.getStatusCode() + ": " + e.getStatusText() + ")");
        }
    }

    @Transactional
    public Cart addToCart(CartDTO cartDTO, String keycloakId) {
        // Gọi ProductService qua gateway để lấy thông tin sản phẩm
        ProductDTO product = getProduct(cartDTO.getProductId());
        if (product == null) {
            throw new CustomException("Product not found with ID: " + cartDTO.getProductId());
        }

        // Kiểm tra stock, xử lý null
        Integer stock = product.getStock();
        if (stock == null) {
            throw new CustomException("Stock information is missing for product ID: " + cartDTO.getProductId());
        }
        if (stock < cartDTO.getQuantity()) {
            throw new CustomException("Insufficient stock for product ID: " + cartDTO.getProductId());
        }

        // Tìm hoặc tạo mới giỏ hàng
        Cart cart = cartRepository.findByKeycloakId(keycloakId)
            .orElseGet(() -> {
                Cart newCart = new Cart();
                newCart.setKeycloakId(keycloakId);
                newCart.setTotalPrice(0.0);
                return cartRepository.save(newCart);
            });

        // Kiểm tra xem sản phẩm đã có trong giỏ hàng chưa
        CartItem existingItem = cart.getCartItems().stream()
            .filter(item -> item.getProductId().equals(cartDTO.getProductId()))
            .findFirst()
            .orElse(null);

        if (existingItem != null) {
            existingItem.setQuantity(existingItem.getQuantity() + cartDTO.getQuantity());
            existingItem.setPrice(product.getPrice() * existingItem.getQuantity());
        } else {
            CartItem cartItem = new CartItem();
            cartItem.setCart(cart);
            cartItem.setProductId(cartDTO.getProductId());
            cartItem.setVendorKeycloakId(product.getKeycloakId());
            cartItem.setQuantity(cartDTO.getQuantity());
            cartItem.setPrice(product.getPrice() * cartDTO.getQuantity());
            cart.getCartItems().add(cartItem);
        }

        // Cập nhật tổng giá
        cart.setTotalPrice(cart.getCartItems().stream()
            .mapToDouble(CartItem::getPrice)
            .sum());

        cart = cartRepository.save(cart);
        log.info("Added product to cart: keycloakId={}, productId={}", keycloakId, cartDTO.getProductId());
        return cart;
    }

    public Cart getCart(String keycloakId) {
        Cart cart = cartRepository.findByKeycloakId(keycloakId)
            .orElseThrow(() -> new CustomException("Cart not found for user: " + keycloakId));
        log.info("Retrieved cart: keycloakId={}", keycloakId);
        return cart;
    }

    @Transactional
    public Cart updateCartItem(String keycloakId, Long productId, Integer quantity) {
        if (quantity < 1) {
            throw new CustomException("Quantity must be at least 1");
        }

        Cart cart = getCart(keycloakId);
        CartItem cartItem = cart.getCartItems().stream()
            .filter(item -> item.getProductId().equals(productId))
            .findFirst()
            .orElseThrow(() -> new CustomException("Cart item not found for product ID: " + productId));

        // Gọi ProductService để kiểm tra tồn kho
        ProductDTO product = getProduct(productId);
        if (product == null) {
            throw new CustomException("Product not found with ID: " + productId);
        }
        Integer stock = product.getStock();
        if (stock == null) {
            throw new CustomException("Stock information is missing for product ID: " + productId);
        }
        if (stock < quantity) {
            throw new CustomException("Insufficient stock for product ID: " + productId);
        }

        cartItem.setQuantity(quantity);
        cartItem.setPrice(product.getPrice() * quantity);
        cart.setTotalPrice(cart.getCartItems().stream()
            .mapToDouble(CartItem::getPrice)
            .sum());

        cartItemRepository.save(cartItem);
        cart = cartRepository.save(cart);
        log.info("Updated cart item: keycloakId={}, productId={}, quantity={}", keycloakId, productId, quantity);
        return cart;
    }

    @Transactional
    public void removeCartItem(String keycloakId, Long productId) {
        Cart cart = getCart(keycloakId);
        CartItem cartItem = cart.getCartItems().stream()
            .filter(item -> item.getProductId().equals(productId))
            .findFirst()
            .orElseThrow(() -> new CustomException("Cart item not found for product ID: " + productId));

        cart.getCartItems().remove(cartItem);
        cart.setTotalPrice(cart.getCartItems().stream()
            .mapToDouble(CartItem::getPrice)
            .sum());

        cartRepository.save(cart);
        log.info("Removed cart item: keycloakId={}, productId={}", keycloakId, productId);
    }
}