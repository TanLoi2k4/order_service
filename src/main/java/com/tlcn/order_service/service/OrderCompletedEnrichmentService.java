package com.tlcn.order_service.service;

import com.tlcn.order_service.client.CustomerClient;
import com.tlcn.order_service.dto.CustomerInternalProfileResponse;
import com.tlcn.order_service.dto.kafka.OrderCompleted;
import com.tlcn.order_service.dto.kafka.OrderItemCompleted;
import com.tlcn.order_service.dto.kafka.SubOrderCompleted;
import com.tlcn.order_service.enums.SubOrderStatus;
import com.tlcn.order_service.model.Order;
import com.tlcn.order_service.model.OrderItem;
import com.tlcn.order_service.model.SubOrder;
import com.tlcn.order_service.repository.SubOrderRepository;
import com.tlcn.order_service.dto.ProductResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderCompletedEnrichmentService {

    private final SubOrderRepository subOrderRepository;
    private final ProductLookupService productLookupService;
    private final CustomerClient customerClient;

    public OrderCompleted buildOrderCompletedEvent(Order order) {
        if (order == null) {
            return null;
        }

        List<SubOrder> subOrders = subOrderRepository.findByOrderIdWithItems(order.getId());
        List<SubOrderCompleted> completedSubOrders = new ArrayList<>();

        for (SubOrder subOrder : subOrders) {
            if (!isChargeableSubOrder(subOrder) || subOrder.getStatus() != SubOrderStatus.COMPLETED) {
                continue;
            }
            completedSubOrders.add(mapSubOrder(subOrder));
        }

        CustomerContact contact = resolveCustomerContact(order);

        return OrderCompleted.builder()
            .eventId(UUID.randomUUID().toString())
            .orderCode(order.getOrderCode())
            .orderId(order.getId())
            .customerKeycloakId(order.getCustomerKeycloakId())
            .customerName(contact.displayName())
            .customerPhone(contact.phone())
            .completedAt(Instant.now())
            .subOrders(completedSubOrders)
            .build();
    }

    private CustomerContact resolveCustomerContact(Order order) {
        String displayName = trimToNull(order.getCustomerName());
        String rawPhone = trimToNull(order.getCustomerPhone());

        if ((displayName == null || rawPhone == null) && order.getCustomerKeycloakId() != null) {
            CustomerInternalProfileResponse profile = customerClient.getProfile(order.getCustomerKeycloakId());
            if (profile != null) {
                if (displayName == null) {
                    displayName = trimToNull(profile.getDisplayName());
                }
                if (rawPhone == null) {
                    rawPhone = trimToNull(profile.getPhone());
                }
            }
        }

        if (displayName == null && order.getCustomerKeycloakId() != null) {
            displayName = "Customer " + order.getCustomerKeycloakId().substring(0, Math.min(8, order.getCustomerKeycloakId().length()));
        }

        return new CustomerContact(displayName, rawPhone);
    }

    private String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private record CustomerContact(String displayName, String phone) {}

    private SubOrderCompleted mapSubOrder(SubOrder subOrder) {
        BigDecimal commission = defaultZero(subOrder.getCommissionFee());
        BigDecimal finalAmount = defaultZero(subOrder.getFinalAmount());
        BigDecimal vendorPayout = finalAmount.subtract(commission).max(BigDecimal.ZERO);

        List<OrderItemCompleted> items = new ArrayList<>();
        if (subOrder.getItems() != null) {
            for (OrderItem item : subOrder.getItems()) {
                items.add(mapOrderItem(item));
            }
        }

        return SubOrderCompleted.builder()
            .subOrderId(subOrder.getId())
            .vendorKeycloakId(subOrder.getVendorKeycloakId())
            .subTotal(defaultZero(subOrder.getSubtotal()))
            .commission(commission)
            .vendorPayout(vendorPayout)
            .items(items)
            .build();
    }

    private OrderItemCompleted mapOrderItem(OrderItem item) {
        String productName = "Product #" + item.getProductId();
        String categoryName = "Uncategorized";

        if (item.getProductId() != null) {
            try {
                ProductResponse product = productLookupService.getProduct(item.getProductId());
                if (product != null) {
                    if (product.getName() != null && !product.getName().isBlank()) {
                        productName = product.getName();
                    }
                    if (product.getCategoryName() != null && !product.getCategoryName().isBlank()) {
                        categoryName = product.getCategoryName();
                    }
                }
            } catch (Exception ex) {
                log.warn("Could not resolve product {} for analytics enrichment: {}", item.getProductId(), ex.getMessage());
            }
        }

        Long quantity = item.getQuantity() == null ? 0L : item.getQuantity().longValue();

        return OrderItemCompleted.builder()
            .productId(item.getProductId())
            .productName(productName)
            .categoryName(categoryName)
            .quantity(quantity)
            .unitPrice(defaultZero(item.getUnitPrice()))
            .lineTotal(defaultZero(item.getLineTotal()))
            .build();
    }

    private boolean isChargeableSubOrder(SubOrder subOrder) {
        if (subOrder == null || subOrder.getStatus() == null) {
            return false;
        }
        return subOrder.getStatus() != SubOrderStatus.CANCELLED
            && subOrder.getStatus() != SubOrderStatus.RETURNED;
    }

    private BigDecimal defaultZero(BigDecimal value) {
        return value == null ? BigDecimal.ZERO : value;
    }
}
