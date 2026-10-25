package com.tlcn.order_service.service;

import com.tlcn.order_service.dto.kafka.DiscountCommand;
import com.tlcn.order_service.dto.kafka.DiscountCommandType;
import com.tlcn.order_service.dto.kafka.DiscountEvent;
import com.tlcn.order_service.exception.BusinessException;
import com.tlcn.order_service.kafka.DiscountCommandProducer;
import com.tlcn.order_service.model.Order;
import com.tlcn.order_service.util.MoneyUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

@Service
@RequiredArgsConstructor
public class DiscountHoldService {

    private final DiscountCommandProducer discountCommandProducer;
    private final DiscountReplyAwaiter discountReplyAwaiter;

    public record DiscountRecalculationResult(BigDecimal discountAmount, Long holdId, boolean holdStillValid) {}

    public DiscountRecalculationResult recalculateHold(
        String userId,
        Order order,
        double remainingSubtotal,
        String reason
    ) {
        String correlationId = UUID.randomUUID().toString();

        DiscountCommand cmd = DiscountCommand.builder()
            .type(DiscountCommandType.HOLD_REQUESTED)
            .commandId(correlationId)
            .orderId(order.getId())
            .userId(userId)
            .code(order.getPlatformDiscountCode())
            .holdId(order.getPlatformDiscountHoldId())
            .subtotal(remainingSubtotal)
            .createdAt(Instant.now())
            .reason(reason)
            .build();

        discountReplyAwaiter.register(correlationId);
        discountCommandProducer.send(cmd, discountCommandProducer.getDiscountRepliesTopic());

        try {
            DiscountEvent event = discountReplyAwaiter.await(correlationId);

            if (event == null || event.getType() == null) {
                throw new BusinessException("Discount service returned an invalid response", HttpStatus.SERVICE_UNAVAILABLE, "DISCOUNT_INVALID_REPLY");
            }

            return switch (event.getType()) {
                case HELD -> new DiscountRecalculationResult(
                    MoneyUtils.safeMoney(event.getDiscountAmount()),
                    event.getHoldId() != null ? event.getHoldId() : order.getPlatformDiscountHoldId(),
                    true
                );
                case HOLD_FAILED -> new DiscountRecalculationResult(
                    BigDecimal.ZERO,
                    order.getPlatformDiscountHoldId(),
                    false
                );
                default -> throw new BusinessException("Discount service returned an unexpected response", HttpStatus.SERVICE_UNAVAILABLE, "DISCOUNT_UNEXPECTED_REPLY");
            };
        } catch (TimeoutException ex) {
            discountReplyAwaiter.cancel(correlationId);
            throw new BusinessException("Discount service is unavailable", HttpStatus.SERVICE_UNAVAILABLE, "DISCOUNT_TIMEOUT");
        } catch (BusinessException ex) {
            discountReplyAwaiter.cancel(correlationId);
            throw ex;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            discountReplyAwaiter.cancel(correlationId);
            throw new BusinessException("Discount service is unavailable", HttpStatus.SERVICE_UNAVAILABLE, "DISCOUNT_UNAVAILABLE");
        } catch (Exception ex) {
            discountReplyAwaiter.cancel(correlationId);
            throw new BusinessException("Discount service is unavailable", HttpStatus.SERVICE_UNAVAILABLE, "DISCOUNT_UNAVAILABLE");
        }
    }
}
