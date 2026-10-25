package com.tlcn.order_service.service;

import com.tlcn.order_service.enums.OrderItemStatus;
import com.tlcn.order_service.model.Order;
import com.tlcn.order_service.model.OrderItem;
import com.tlcn.order_service.model.SubOrder;
import com.tlcn.order_service.util.MoneyUtils;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Service
public class OrderAmountService {

    public void recomputeOrderAmountsFromItemStates(Order order, BigDecimal platformDiscountAmount) {
        BigDecimal baseTotal = BigDecimal.ZERO;

        for (SubOrder subOrder : Optional.ofNullable(order.getSubOrders()).orElseGet(List::of)) {
            BigDecimal subtotal = Optional.ofNullable(subOrder.getItems()).orElseGet(List::of).stream()
                .filter(Objects::nonNull)
                .filter(i -> i.getStatus() != OrderItemStatus.CANCELLED)
                .map(OrderItem::getLineTotal)
                .filter(Objects::nonNull)
                .reduce(BigDecimal.ZERO, BigDecimal::add);

            subOrder.setSubtotal(subtotal);

            if (subtotal.compareTo(BigDecimal.ZERO) <= 0) {
                subOrder.setFinalAmount(BigDecimal.ZERO);
                continue;
            }

            BigDecimal finalAmount = subtotal;
            if (finalAmount.compareTo(BigDecimal.ZERO) < 0) {
                finalAmount = BigDecimal.ZERO;
            }

            subOrder.setFinalAmount(finalAmount);
            baseTotal = baseTotal.add(finalAmount);
        }

        BigDecimal normalizedDiscount = MoneyUtils.normalizeMoney(platformDiscountAmount);

        order.setPlatformDiscountAmount(normalizedDiscount);
        order.setDiscountAmount(normalizedDiscount);

        BigDecimal totalPayable = baseTotal.subtract(normalizedDiscount);
        if (totalPayable.compareTo(BigDecimal.ZERO) < 0) {
            totalPayable = BigDecimal.ZERO;
        }
        order.setTotalPayableAmount(totalPayable.setScale(2, RoundingMode.HALF_UP));
    }

    public double sumActiveItemSubtotal(Order order) {
        BigDecimal subtotal = Optional.ofNullable(order.getSubOrders()).orElseGet(List::of).stream()
            .flatMap(so -> Optional.ofNullable(so.getItems()).orElseGet(List::of).stream())
            .filter(Objects::nonNull)
            .filter(i -> i.getStatus() != OrderItemStatus.CANCELLED)
            .map(OrderItem::getLineTotal)
            .filter(Objects::nonNull)
            .reduce(BigDecimal.ZERO, BigDecimal::add);

        return subtotal.doubleValue();
    }
}
