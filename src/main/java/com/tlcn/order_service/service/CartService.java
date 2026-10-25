package com.tlcn.order_service.service;

import com.tlcn.order_service.dto.CartRequest;
import com.tlcn.order_service.dto.ProductResponse;
import com.tlcn.order_service.exception.BusinessException;
import com.tlcn.order_service.exception.ResourceNotFoundException;
import com.tlcn.order_service.model.Cart;
import com.tlcn.order_service.model.CartItem;
import com.tlcn.order_service.repository.CartRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Objects;
import java.util.Optional;

@Service
@RequiredArgsConstructor
@Slf4j
public class CartService {

    private final CartRepository cartRepository;
    private final ProductLookupService productLookupService;

    public Cart getOrCreateCart(String keycloakId) {
        return cartRepository.findByKeycloakId(keycloakId)
            .orElseGet(() -> {
                Cart newCart = Cart.builder()
                    .keycloakId(keycloakId)
                    .totalPrice(BigDecimal.ZERO)
                    .build();
                return cartRepository.save(newCart);
            });
    }

    @Transactional
    public Cart addToCart(CartRequest request, String keycloakId) {
        int requestedQuantity = requirePositiveQuantity(request.getQuantity());

        Cart cart = cartRepository.findByKeycloakIdWithLock(keycloakId)
            .orElseGet(() -> cartRepository.save(Cart.builder()
                .keycloakId(keycloakId)
                .totalPrice(BigDecimal.ZERO)
                .build()));

        ProductResponse product = productLookupService.getProduct(request.getProductId());
        BigDecimal unitPrice = unitPriceOf(product);

        Optional<CartItem> existingItem = cart.getCartItems().stream()
            .filter(item -> Objects.equals(item.getProductId(), request.getProductId()))
            .findFirst();

        if (existingItem.isPresent()) {
            CartItem item = existingItem.get();
            int currentQuantity = Optional.ofNullable(item.getQuantity()).orElse(0);
            item.setQuantity(currentQuantity + requestedQuantity);
            item.setVendorKeycloakId(product.getKeycloakId());
            // Đơn giá / sản phẩm — không nhân theo số lượng
            item.setPrice(unitPrice);
        } else {
            CartItem newItem = CartItem.builder()
                .cart(cart)
                .productId(request.getProductId())
                .vendorKeycloakId(product.getKeycloakId())
                .quantity(requestedQuantity)
                .price(unitPrice)
                .build();
            cart.getCartItems().add(newItem);
        }

        recalculateTotal(cart);
        return cartRepository.save(cart);
    }

    @Transactional
    public Cart removeFromCart(Long productId, String keycloakId) {
        Cart cart = cartRepository.findByKeycloakIdWithLock(keycloakId)
            .orElseThrow(() -> new ResourceNotFoundException("Cart not found"));

        cart.getCartItems().removeIf(item -> Objects.equals(item.getProductId(), productId));
        recalculateTotal(cart);
        return cartRepository.save(cart);
    }

    @Transactional
    public Cart updateCartItem(CartRequest request, String keycloakId) {
        int requestedQuantity = requirePositiveQuantity(request.getQuantity());

        Cart cart = cartRepository.findByKeycloakIdWithLock(keycloakId)
            .orElseThrow(() -> new ResourceNotFoundException("Cart not found"));

        CartItem item = cart.getCartItems().stream()
            .filter(i -> Objects.equals(i.getProductId(), request.getProductId()))
            .findFirst()
            .orElseThrow(() -> new ResourceNotFoundException(
                "CartItem not found for productId=" + request.getProductId()));

        ProductResponse product = productLookupService.getProduct(request.getProductId());

        item.setQuantity(requestedQuantity);
        item.setVendorKeycloakId(product.getKeycloakId());
        // Đơn giá cố định theo sản phẩm; line total = price * quantity khi tính tổng
        item.setPrice(unitPriceOf(product));

        recalculateTotal(cart);
        return cartRepository.save(cart);
    }

    /**
     * Lấy giỏ và đồng bộ lại đơn giá từ product-service
     * (sửa dữ liệu cũ từng lưu nhầm line total vào cột price).
     */
    @Transactional
    public Cart getCart(String keycloakId) {
        Cart cart = getOrCreateCart(keycloakId);
        boolean changed = refreshUnitPrices(cart);
        if (changed) {
            recalculateTotal(cart);
            return cartRepository.save(cart);
        }
        // Đảm bảo total khớp price * quantity kể cả khi đơn giá không đổi
        BigDecimal expected = sumLineTotals(cart);
        if (cart.getTotalPrice() == null || cart.getTotalPrice().compareTo(expected) != 0) {
            cart.setTotalPrice(expected);
            return cartRepository.save(cart);
        }
        return cart;
    }

    private boolean refreshUnitPrices(Cart cart) {
        boolean changed = false;
        if (cart.getCartItems() == null) {
            return false;
        }
        for (CartItem item : cart.getCartItems()) {
            if (item.getProductId() == null) {
                continue;
            }
            try {
                ProductResponse product = productLookupService.getProduct(item.getProductId());
                BigDecimal unitPrice = unitPriceOf(product);
                if (item.getPrice() == null || item.getPrice().compareTo(unitPrice) != 0) {
                    item.setPrice(unitPrice);
                    changed = true;
                }
                if (product.getKeycloakId() != null
                    && !Objects.equals(item.getVendorKeycloakId(), product.getKeycloakId())) {
                    item.setVendorKeycloakId(product.getKeycloakId());
                    changed = true;
                }
            } catch (Exception ex) {
                log.warn("Could not refresh unit price for productId={}: {}", item.getProductId(), ex.getMessage());
            }
        }
        return changed;
    }

    private static int requirePositiveQuantity(Integer quantity) {
        if (quantity == null || quantity <= 0) {
            throw new BusinessException("Quantity must be greater than 0", HttpStatus.BAD_REQUEST, "INVALID_QUANTITY");
        }
        return quantity;
    }

    private static BigDecimal unitPriceOf(ProductResponse product) {
        if (product.getPrice() == null) {
            throw new BusinessException("Product price is missing", HttpStatus.BAD_REQUEST, "PRODUCT_PRICE_MISSING");
        }
        return BigDecimal.valueOf(product.getPrice());
    }

    /** Tổng giỏ = Σ (đơn giá × số lượng). */
    private static void recalculateTotal(Cart cart) {
        cart.setTotalPrice(sumLineTotals(cart));
    }

    private static BigDecimal sumLineTotals(Cart cart) {
        if (cart.getCartItems() == null || cart.getCartItems().isEmpty()) {
            return BigDecimal.ZERO;
        }
        return cart.getCartItems().stream()
            .map(CartService::lineTotal)
            .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    private static BigDecimal lineTotal(CartItem item) {
        BigDecimal unit = Optional.ofNullable(item.getPrice()).orElse(BigDecimal.ZERO);
        int qty = Optional.ofNullable(item.getQuantity()).orElse(0);
        if (qty <= 0) {
            return BigDecimal.ZERO;
        }
        return unit.multiply(BigDecimal.valueOf(qty));
    }
}
