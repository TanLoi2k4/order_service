package com.tlcn.order_service.service;

import com.tlcn.order_service.dto.CreateOrderRequest;
import com.tlcn.order_service.enums.PaymentStatus;
import com.tlcn.order_service.enums.SubOrderStatus;
import com.tlcn.order_service.exception.BusinessException;
import com.tlcn.order_service.exception.ResourceNotFoundException;
import com.tlcn.order_service.model.Cart;
import com.tlcn.order_service.model.CartItem;
import com.tlcn.order_service.model.Order;
import com.tlcn.order_service.model.OrderItem;
import com.tlcn.order_service.model.SubOrder;
import com.tlcn.order_service.repository.CartRepository;
import com.tlcn.order_service.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Separate bean so {@code @Transactional} on checkout persistence is applied via Spring proxy
 * (avoids self-invocation inside {@link CustomerOrderService}).
 */
@Service
@RequiredArgsConstructor
public class OrderCheckoutWriter {

    private final CartRepository cartRepository;
    private final OrderRepository orderRepository;

    @Transactional(readOnly = true)
    public CartSnapshot loadCartSnapshot(String customerKeycloakId) {
        Cart cart = cartRepository.findByKeycloakId(customerKeycloakId)
            .orElseThrow(() -> new ResourceNotFoundException("Cart not found"));

        if (cart.getCartItems() == null || cart.getCartItems().isEmpty()) {
            throw new BusinessException("Cart is empty", HttpStatus.BAD_REQUEST, "CART_EMPTY");
        }

        // CartItem.price = đơn giá; subtotal = Σ (price × quantity)
        double subtotal = cart.getCartItems().stream()
            .mapToDouble(item -> {
                BigDecimal unit = Optional.ofNullable(item.getPrice()).orElse(BigDecimal.ZERO);
                int qty = Optional.ofNullable(item.getQuantity()).orElse(0);
                if (qty <= 0) {
                    return 0d;
                }
                return unit.multiply(BigDecimal.valueOf(qty)).doubleValue();
            })
            .sum();

        return new CartSnapshot(cart.getId(), cart.getVersion(), subtotal);
    }

    @Transactional
    public Order createOrderFromCart(
        String customerKeycloakId,
        CreateOrderRequest request,
        DiscountHoldData holdReply,
        Long expectedCartVersion
    ) {
        // Pessimistic lock prevents concurrent cart mutations during checkout
        Cart cart = cartRepository.findByKeycloakIdWithLock(customerKeycloakId)
            .orElseThrow(() -> new ResourceNotFoundException("Cart not found"));

        if (expectedCartVersion != null && !Objects.equals(cart.getVersion(), expectedCartVersion)) {
            throw new BusinessException(
                "Cart was modified during checkout. Please retry.",
                HttpStatus.CONFLICT,
                "CART_VERSION_CONFLICT"
            );
        }

        if (cart.getCartItems() == null || cart.getCartItems().isEmpty()) {
            throw new BusinessException("Cart is empty", HttpStatus.BAD_REQUEST, "CART_EMPTY");
        }

        if (request.getPaymentMethod() == null) {
            throw new BusinessException("Payment method is required", HttpStatus.BAD_REQUEST, "PAYMENT_METHOD_REQUIRED");
        }

        Order order = Order.builder()
            .customerKeycloakId(customerKeycloakId)
            .platformDiscountCode(request.getPlatformDiscountCode())
            .paymentMethod(request.getPaymentMethod())
            .shippingAddress(request.getShippingAddress())
            .customerName(request.getCustomerName())
            .customerPhone(request.getCustomerPhone())
            .note(request.getNote())
            .build();

        if (holdReply != null) {
            order.setPlatformDiscountHoldId(holdReply.holdId());
            order.setPlatformDiscountAmount(holdReply.discountAmount());
            order.setDiscountAmount(holdReply.discountAmount());
        }

        Map<String, SubOrder> subOrderByVendor = new LinkedHashMap<>();

        for (CartItem cartItem : cart.getCartItems()) {
            if (cartItem.getProductId() == null) {
                throw new BusinessException("Invalid cart item", HttpStatus.BAD_REQUEST, "INVALID_CART_ITEM");
            }

            Integer quantityObj = cartItem.getQuantity();
            if (quantityObj == null || quantityObj <= 0) {
                throw new BusinessException("Quantity must be > 0", HttpStatus.BAD_REQUEST, "INVALID_QUANTITY");
            }

            String vendorKeycloakId = cartItem.getVendorKeycloakId();
            if (vendorKeycloakId == null || vendorKeycloakId.isBlank()) {
                throw new BusinessException("Cart item missing vendor", HttpStatus.BAD_REQUEST, "CART_ITEM_VENDOR_MISSING");
            }

            SubOrder subOrder = subOrderByVendor.computeIfAbsent(vendorKeycloakId, vendor -> SubOrder.builder()
                .order(order)
                .vendorKeycloakId(vendor)
                .status(SubOrderStatus.CREATED)
                .subtotal(BigDecimal.ZERO)
                .finalAmount(BigDecimal.ZERO)
                .platformDiscountShare(BigDecimal.ZERO)
                .commissionFee(BigDecimal.ZERO)
                .build());

            // CartItem.price = đơn giá / sản phẩm (không phải line total)
            BigDecimal unitPrice = Optional.ofNullable(cartItem.getPrice()).orElse(BigDecimal.ZERO);
            int quantity = quantityObj;
            BigDecimal lineTotal = unitPrice
                .multiply(BigDecimal.valueOf(quantity))
                .setScale(2, RoundingMode.HALF_UP);

            OrderItem orderItem = OrderItem.builder()
                .subOrder(subOrder)
                .productId(cartItem.getProductId())
                .quantity(quantity)
                .unitPrice(unitPrice)
                .lineTotal(lineTotal)
                .build();

            subOrder.getItems().add(orderItem);
            subOrder.setSubtotal(subOrder.getSubtotal().add(lineTotal));
        }

        BigDecimal total = BigDecimal.ZERO;
        for (SubOrder subOrder : subOrderByVendor.values()) {
            BigDecimal discountShare = Optional.ofNullable(subOrder.getPlatformDiscountShare()).orElse(BigDecimal.ZERO);
            BigDecimal finalAmount = subOrder.getSubtotal().subtract(discountShare);
            if (finalAmount.compareTo(BigDecimal.ZERO) < 0) {
                finalAmount = BigDecimal.ZERO;
            }
            subOrder.setFinalAmount(finalAmount);
            total = total.add(finalAmount);

            order.getSubOrders().add(subOrder);
        }

        if (order.getPlatformDiscountAmount() == null) {
            order.setPlatformDiscountAmount(BigDecimal.ZERO);
        }

        BigDecimal totalPayable = total.subtract(order.getPlatformDiscountAmount());
        if (totalPayable.compareTo(BigDecimal.ZERO) < 0) {
            totalPayable = BigDecimal.ZERO;
        }

        order.setTotalPayableAmount(totalPayable);

        if (order.getPaymentStatus() == null) {
            order.setPaymentStatus(PaymentStatus.PENDING);
        }

        Order saved = orderRepository.save(order);

        cart.getCartItems().clear();
        cart.setTotalPrice(BigDecimal.ZERO);
        cartRepository.save(cart);

        return saved;
    }

    public record CartSnapshot(Long cartId, Long version, double subtotal) {}

    public record DiscountHoldData(Long holdId, BigDecimal discountAmount) {}
}
