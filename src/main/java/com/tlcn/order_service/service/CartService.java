package com.tlcn.order_service.service;

import com.tlcn.order_service.model.Cart;
import com.tlcn.order_service.model.CartItem;
import com.tlcn.order_service.dto.OrderRequestDTO;
import com.tlcn.order_service.dto.OrderResponseDTO;
import com.tlcn.order_service.dto.OrderItemDTO;
import com.tlcn.order_service.model.Status; 
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate; 

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class CartService {
    private static final String CART_KEY_PREFIX = "cart:customer:";
    private static final long CART_TTL_MINUTES = 60 * 24; // Giỏ hàng tồn tại 24 giờ
    
    @Qualifier("cartRedisTemplate") 
    private final RedisTemplate<String, Object> redisTemplate;
    private final OrderService orderService;
    private final RestTemplate restTemplate; // Giả định dùng RestTemplate cho liên lạc nội bộ
    
    // =========================================================================
    // REDIS / CART CORE LOGIC
    // =========================================================================

    /**
     * Lấy Cart của Customer từ Redis. Nếu không có, tạo Cart rỗng.
     */
    public Cart getCart(Long customerId) {
        String cartKey = CART_KEY_PREFIX + customerId;
        
        // Lấy Cart từ Redis. Vì lưu Object, cần ép kiểu.
        Cart cart = (Cart) redisTemplate.opsForValue().get(cartKey);

        if (cart == null) {
            cart = new Cart(customerId);
        }
        
        // Không cần recalculate ở đây nếu logic thêm/xóa đã tính toán.
        return cart;
    }

    /**
     * Thêm sản phẩm mới hoặc cập nhật số lượng của sản phẩm đã tồn tại.
     */
    @Transactional
    public Cart addItemToCart(Long customerId, CartItem newItem, int quantity) {
        
        if (quantity <= 0) {
            throw new OrderService.CustomException("Quantity must be greater than zero.");
        }
        
        // Lấy thông tin giá/vendor (GỌI ĐẾN PRODUCT SERVICE - CẦN THAY BẰNG REST CALL THỰC TẾ)
        ProductInfo productInfo = getProductInfo(newItem.getProductId());
        
        // Đảm bảo thông tin Item là chính xác và không bị người dùng giả mạo
        newItem.setPrice(productInfo.price());
        newItem.setVendorId(productInfo.vendorId());
        newItem.setProductName(productInfo.name());
        newItem.setQuantity(quantity); // Chỉ lấy quantity từ đầu vào

        Cart cart = getCart(customerId);
        
        // Bắt buộc: Giỏ hàng chỉ chấp nhận sản phẩm từ MỘT vendor
        if (!cart.getItems().isEmpty()) {
            Long existingVendorId = cart.getItems().get(0).getVendorId();
            if (existingVendorId != null && !existingVendorId.equals(newItem.getVendorId())) {
                 throw new OrderService.CustomException("Cart can only contain items from one vendor (" + existingVendorId + " vs " + newItem.getVendorId() + ")");
            }
        }
        
        // 1. Kiểm tra Item đã có chưa
        Optional<CartItem> existingItem = cart.getItems().stream()
            .filter(item -> item.getProductId().equals(newItem.getProductId()))
            .findFirst();

        if (existingItem.isPresent()) {
            // Cập nhật số lượng
            existingItem.get().setQuantity(existingItem.get().getQuantity() + quantity);
            log.info("Updated quantity for productId {} in cart of customer {}", newItem.getProductId(), customerId);
        } else {
            // Thêm mới (nếu CartItem đã được tạo đúng)
            cart.getItems().add(newItem);
            log.info("Added new item productId {} to cart of customer {}", newItem.getProductId(), customerId);
        }

        // 2. Tính toán lại và lưu vào Redis
        cart.recalculate();
        saveCart(cart);
        
        return cart;
    }
    
    /**
     * Cập nhật số lượng item
     */
    @Transactional
    public Cart updateItemQuantity(Long customerId, String itemId, int newQuantity) {
        if (newQuantity <= 0) {
            return removeItemFromCart(customerId, itemId); // Xóa nếu số lượng = 0
        }
        
        Cart cart = getCart(customerId);
        
        cart.getItems().stream()
            .filter(item -> item.getItemId().equals(itemId))
            .findFirst()
            .ifPresentOrElse(
                item -> item.setQuantity(newQuantity),
                () -> { throw new OrderService.CustomException("Item not found in cart: " + itemId); }
            );

        cart.recalculate();
        saveCart(cart);
        
        return cart;
    }
    
    /**
     * Xóa một item khỏi giỏ hàng
     */
    @Transactional
    public Cart removeItemFromCart(Long customerId, String itemId) {
        Cart cart = getCart(customerId);
        
        boolean removed = cart.getItems().removeIf(item -> item.getItemId().equals(itemId));
        
        if (!removed) {
            throw new OrderService.CustomException("Item not found in cart: " + itemId);
        }
        
        cart.recalculate();
        saveCart(cart);
        
        return cart;
    }

    /**
     * Xóa toàn bộ giỏ hàng
     */
    public void clearCart(Long customerId) {
         redisTemplate.delete(CART_KEY_PREFIX + customerId);
         log.info("Cart cleared for customerId: {}", customerId);
    }
    
    private void saveCart(Cart cart) {
        String cartKey = CART_KEY_PREFIX + cart.getCustomerId();
        redisTemplate.opsForValue().set(cartKey, cart, CART_TTL_MINUTES, TimeUnit.MINUTES);
    }

    // =========================================================================
    // CHECKOUT LOGIC
    // =========================================================================
    
    /**
     * Chuyển Giỏ hàng thành Đơn hàng (SAGA KICKOFF)
     */
    @Transactional
    public OrderResponseDTO checkoutCart(Long customerId, String shippingAddress, String keycloakId) {
        Cart cart = getCart(customerId);
        
        if (cart.getItems().isEmpty()) {
            throw new OrderService.CustomException("Cart is empty. Cannot checkout.");
        }
        
        // 1. Kiểm tra Vendor ID (Phải có ít nhất 1 item)
        Long vendorId = cart.getItems().get(0).getVendorId(); 
        if (vendorId == null) {
            throw new OrderService.CustomException("Cart items must belong to a vendor.");
        }

        // 2. Chuyển Cart sang OrderRequestDTO
        OrderRequestDTO request = new OrderRequestDTO();
        request.setCustomerId(customerId);
        request.setVendorId(vendorId); 
        request.setTotalAmount(cart.getTotalAmount());
        request.setShippingAddress(shippingAddress);
        
        List<OrderItemDTO> orderItems = cart.getItems().stream().map(cartItem -> {
            OrderItemDTO orderItem = new OrderItemDTO();
            orderItem.setProductId(cartItem.getProductId());
            orderItem.setQuantity(cartItem.getQuantity());
            orderItem.setPrice(cartItem.getPrice());
            return orderItem;
        }).collect(Collectors.toList());
        request.setItems(orderItems);

        // 3. Gọi logic tạo đơn hàng (kích hoạt SAGA)
        OrderResponseDTO response = orderService.createOrder(request, keycloakId);
        
        // 4. XÓA Cart sau khi tạo Order THÀNH CÔNG
        clearCart(customerId);

        return response;
    }

    // =========================================================================
    // UTILITY/MOCK METHODS (CẦN THAY THẾ)
    // =========================================================================

    // Dùng để lấy thông tin (ID nội bộ) từ Keycloak ID
    public Long getCustomerIdFromKeycloakId(String keycloakId) {
        // NOTE: Sử dụng OrderService để tránh trùng lặp logic
        return orderService.getCustomerIdFromKeycloakId(keycloakId);
    }

    // Lớp nội bộ để giả lập thông tin sản phẩm
    private record ProductInfo(Long productId, String name, Long vendorId, BigDecimal price) {}

    // GIẢ LẬP: Cần thay thế bằng REST call tới Product Service
    private ProductInfo getProductInfo(Long productId) {
        // Đây là nơi bạn sẽ gọi RestTemplate:
        // restTemplate.getForObject("http://product-service/api/products/" + productId, ProductResponse.class);
        
        // Giả lập data:
        if (productId.equals(101L)) {
            return new ProductInfo(101L, "Laptop X", 201L, new BigDecimal("1500.00"));
        } else if (productId.equals(102L)) {
            return new ProductInfo(102L, "Mouse Y", 201L, new BigDecimal("25.00"));
        } else {
            // Xử lý lỗi (Nếu sản phẩm không tồn tại)
            throw new OrderService.CustomException("Product not found or invalid: " + productId);
        }
    }
}