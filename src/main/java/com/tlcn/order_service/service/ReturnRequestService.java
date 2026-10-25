package com.tlcn.order_service.service;

import com.tlcn.order_service.dto.CreateReturnRequest;
import com.tlcn.order_service.dto.PageResponse;
import com.tlcn.order_service.dto.RefundRequest;
import com.tlcn.order_service.dto.ReturnRequestResponse;
import com.tlcn.order_service.dto.kafka.ReturnRequestChanged;
import com.tlcn.order_service.dto.kafka.StockRollbackRequest;
import com.tlcn.order_service.dto.kafka.SubOrderShippingRequested;
import com.tlcn.order_service.enums.OrderItemStatus;
import com.tlcn.order_service.enums.PaymentStatus;
import com.tlcn.order_service.enums.PayoutStatus;
import com.tlcn.order_service.enums.ReturnRequestStatus;
import com.tlcn.order_service.enums.SubOrderStatus;
import com.tlcn.order_service.exception.BusinessException;
import com.tlcn.order_service.exception.ResourceNotFoundException;
import com.tlcn.order_service.kafka.OrderEventProducer;
import com.tlcn.order_service.model.Order;
import com.tlcn.order_service.model.OrderItem;
import com.tlcn.order_service.model.ReturnRequest;
import com.tlcn.order_service.model.ReturnRequestItem;
import com.tlcn.order_service.model.SubOrder;
import com.tlcn.order_service.repository.OrderRepository;
import com.tlcn.order_service.repository.ReturnRequestRepository;
import com.tlcn.order_service.repository.SubOrderRepository;
import com.tlcn.order_service.util.MoneyUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class ReturnRequestService {

    private final ReturnRequestRepository returnRequestRepository;
    private final OrderRepository orderRepository;
    private final SubOrderRepository subOrderRepository;
    private final ReturnRequestMapper returnRequestMapper;
    private final OrderEventProducer orderEventProducer;

    @Value("${return.window-days:7}")
    private long windowDays;

    @Value("${return.auto-approve-hours:48}")
    private long autoApproveHours;

    @Transactional
    public ReturnRequestResponse create(
        String customerKeycloakId,
        String orderCode,
        Long subOrderId,
        CreateReturnRequest request
    ) {
        if (request == null || request.getItemIds() == null || request.getItemIds().isEmpty()) {
            throw new BusinessException("At least one item is required", HttpStatus.BAD_REQUEST, "RETURN_ITEMS_REQUIRED");
        }
        if (request.getReason() == null) {
            throw new BusinessException("Return reason is required", HttpStatus.BAD_REQUEST, "RETURN_REASON_REQUIRED");
        }

        Order order = orderRepository.findByOrderCodeAndCustomerKeycloakId(orderCode, customerKeycloakId)
            .orElseThrow(() -> new ResourceNotFoundException("Order not found"));

        SubOrder subOrder = Optional.ofNullable(order.getSubOrders()).orElseGet(List::of).stream()
            .filter(so -> so.getId() != null && so.getId().equals(subOrderId))
            .findFirst()
            .orElseThrow(() -> new ResourceNotFoundException("Sub-order not found"));

        if (subOrder.getStatus() != SubOrderStatus.COMPLETED) {
            throw new BusinessException("Chỉ được trả hàng sau khi đã nhận", HttpStatus.BAD_REQUEST, "RETURN_NOT_ELIGIBLE");
        }

        Instant deliveredAt = subOrder.getCompletedAt();
        if (deliveredAt != null && Instant.now().isAfter(deliveredAt.plus(Duration.ofDays(Math.max(1L, windowDays))))) {
            throw new BusinessException("Đã hết hạn trả hàng (7 ngày)", HttpStatus.BAD_REQUEST, "RETURN_WINDOW_EXPIRED");
        }

        if (returnRequestRepository.existsBySubOrder_IdAndStatusIn(subOrder.getId(), ReturnRequestStatus.OPEN)) {
            throw new BusinessException("Đơn shop này đang có yêu cầu trả hàng", HttpStatus.CONFLICT, "RETURN_ALREADY_OPEN");
        }

        Map<Long, OrderItem> itemsById = Optional.ofNullable(subOrder.getItems()).orElseGet(List::of).stream()
            .filter(item -> item != null && item.getId() != null)
            .collect(Collectors.toMap(OrderItem::getId, Function.identity(), (a, b) -> a));

        Set<Long> selectedIds = new HashSet<>();
        List<ReturnRequestItem> returnItems = new java.util.ArrayList<>();
        BigDecimal refundAmount = BigDecimal.ZERO;

        for (Long itemId : request.getItemIds()) {
            if (itemId == null || !selectedIds.add(itemId)) {
                continue;
            }
            OrderItem item = itemsById.get(itemId);
            if (item == null) {
                throw new BusinessException("Sản phẩm không thuộc đơn shop này", HttpStatus.BAD_REQUEST, "RETURN_ITEM_INVALID");
            }
            if (item.getStatus() != OrderItemStatus.CONFIRMED) {
                throw new BusinessException("Chỉ trả được sản phẩm đã nhận", HttpStatus.BAD_REQUEST, "RETURN_ITEM_NOT_RETURNABLE");
            }
            BigDecimal lineTotal = MoneyUtils.normalizeMoney(item.getLineTotal());
            refundAmount = refundAmount.add(lineTotal);
            returnItems.add(ReturnRequestItem.builder()
                .orderItemId(item.getId())
                .productId(item.getProductId())
                .quantity(item.getQuantity())
                .unitPrice(MoneyUtils.normalizeMoney(item.getUnitPrice()))
                .lineTotal(lineTotal)
                .build());
        }

        if (returnItems.isEmpty() || refundAmount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new BusinessException("Không có sản phẩm hợp lệ để trả", HttpStatus.BAD_REQUEST, "RETURN_ITEMS_REQUIRED");
        }

        ReturnRequest returnRequest = ReturnRequest.builder()
            .subOrder(subOrder)
            .orderCode(order.getOrderCode())
            .customerKeycloakId(customerKeycloakId)
            .vendorKeycloakId(subOrder.getVendorKeycloakId())
            .status(ReturnRequestStatus.REQUESTED)
            .reason(request.getReason())
            .description(trimToNull(request.getDescription()))
            .refundAmount(MoneyUtils.normalizeMoney(refundAmount))
            .currency(order.getCurrency() == null ? "VND" : order.getCurrency())
            .build();

        for (ReturnRequestItem item : returnItems) {
            item.setReturnRequest(returnRequest);
            returnRequest.getItems().add(item);
        }

        ReturnRequest saved = returnRequestRepository.save(returnRequest);
        publishChanged(saved, null);
        return returnRequestMapper.toResponse(saved);
    }

    @Transactional(readOnly = true)
    public List<ReturnRequestResponse> listForCustomer(String customerKeycloakId, String orderCode) {
        orderRepository.findByOrderCodeAndCustomerKeycloakId(orderCode, customerKeycloakId)
            .orElseThrow(() -> new ResourceNotFoundException("Order not found"));
        return returnRequestRepository
            .findByOrderCodeAndCustomerKeycloakIdOrderByCreatedAtDesc(orderCode, customerKeycloakId)
            .stream()
            .map(returnRequestMapper::toResponse)
            .toList();
    }

    @Transactional(readOnly = true)
    public PageResponse<ReturnRequestResponse> listForVendor(
        String vendorKeycloakId,
        ReturnRequestStatus status,
        int page,
        int size
    ) {
        Page<ReturnRequest> result = status == null
            ? returnRequestRepository.findByVendorKeycloakId(vendorKeycloakId, pageable(page, size))
            : returnRequestRepository.findByVendorKeycloakIdAndStatus(vendorKeycloakId, status, pageable(page, size));
        return toPage(result);
    }

    @Transactional(readOnly = true)
    public ReturnRequestResponse getForVendor(String vendorKeycloakId, Long returnId) {
        return returnRequestMapper.toResponse(requireVendorReturn(vendorKeycloakId, returnId));
    }

    @Transactional
    public ReturnRequestResponse approveByVendor(String vendorKeycloakId, Long returnId, String note) {
        ReturnRequest returnRequest = requireVendorReturn(vendorKeycloakId, returnId);
        return approve(returnRequest, trimToNull(note), false);
    }

    @Transactional
    public ReturnRequestResponse rejectByVendor(String vendorKeycloakId, Long returnId, String note) {
        ReturnRequest returnRequest = requireVendorReturn(vendorKeycloakId, returnId);
        if (returnRequest.getStatus() != ReturnRequestStatus.REQUESTED) {
            throw new BusinessException("Chỉ từ chối được yêu cầu đang chờ duyệt", HttpStatus.BAD_REQUEST, "RETURN_NOT_REQUESTED");
        }
        if (note == null || note.isBlank()) {
            throw new BusinessException("Vui lòng nhập lý do từ chối", HttpStatus.BAD_REQUEST, "RETURN_REJECT_NOTE_REQUIRED");
        }
        ReturnRequestStatus previous = returnRequest.getStatus();
        returnRequest.setStatus(ReturnRequestStatus.REJECTED);
        returnRequest.setSellerNote(note.trim());
        ReturnRequest saved = returnRequestRepository.save(returnRequest);
        publishChanged(saved, previous);
        return returnRequestMapper.toResponse(saved);
    }

    @Transactional
    public ReturnRequestResponse escalate(String customerKeycloakId, String orderCode, Long returnId) {
        ReturnRequest returnRequest = requireCustomerReturn(customerKeycloakId, orderCode, returnId);
        if (returnRequest.getStatus() != ReturnRequestStatus.REJECTED) {
            throw new BusinessException("Chỉ khiếu nại được yêu cầu đã bị từ chối", HttpStatus.BAD_REQUEST, "RETURN_NOT_REJECTED");
        }
        ReturnRequestStatus previous = returnRequest.getStatus();
        returnRequest.setStatus(ReturnRequestStatus.DISPUTED);
        ReturnRequest saved = returnRequestRepository.save(returnRequest);
        publishChanged(saved, previous);
        return returnRequestMapper.toResponse(saved);
    }

    @Transactional(readOnly = true)
    public PageResponse<ReturnRequestResponse> listForAdmin(ReturnRequestStatus status, int page, int size) {
        Page<ReturnRequest> result = status == null
            ? returnRequestRepository.findAllByOrderByCreatedAtDesc(pageable(page, size))
            : returnRequestRepository.findByStatus(status, pageable(page, size));
        return toPage(result);
    }

    @Transactional
    public ReturnRequestResponse approveByAdmin(Long returnId, String note) {
        ReturnRequest returnRequest = requireReturn(returnId);
        if (returnRequest.getStatus() != ReturnRequestStatus.DISPUTED
            && returnRequest.getStatus() != ReturnRequestStatus.REJECTED) {
            throw new BusinessException("Admin chỉ duyệt yêu cầu bị từ chối hoặc tranh chấp", HttpStatus.BAD_REQUEST, "RETURN_NOT_DISPUTED");
        }
        returnRequest.setAdminNote(trimToNull(note));
        return approve(returnRequest, returnRequest.getSellerNote(), true);
    }

    @Transactional
    public ReturnRequestResponse rejectByAdmin(Long returnId, String note) {
        ReturnRequest returnRequest = requireReturn(returnId);
        if (returnRequest.getStatus() != ReturnRequestStatus.DISPUTED
            && returnRequest.getStatus() != ReturnRequestStatus.REJECTED) {
            throw new BusinessException("Admin chỉ đóng yêu cầu bị từ chối hoặc tranh chấp", HttpStatus.BAD_REQUEST, "RETURN_NOT_DISPUTED");
        }
        if (note == null || note.isBlank()) {
            throw new BusinessException("Vui lòng nhập lý do", HttpStatus.BAD_REQUEST, "RETURN_ADMIN_NOTE_REQUIRED");
        }
        ReturnRequestStatus previous = returnRequest.getStatus();
        returnRequest.setStatus(ReturnRequestStatus.CLOSED);
        returnRequest.setAdminNote(note.trim());
        ReturnRequest saved = returnRequestRepository.save(returnRequest);
        publishChanged(saved, previous);
        return returnRequestMapper.toResponse(saved);
    }

    @Transactional
    public int autoApproveExpired() {
        Instant cutoff = Instant.now().minus(Duration.ofHours(Math.max(1L, autoApproveHours)));
        List<ReturnRequest> expired = returnRequestRepository.findByStatusAndCreatedAtBefore(
            ReturnRequestStatus.REQUESTED,
            cutoff
        );
        int count = 0;
        for (ReturnRequest returnRequest : expired) {
            try {
                approve(returnRequest, "Tự động đồng ý vì shop không phản hồi", false);
                count++;
            } catch (Exception ex) {
                log.warn("Auto-approve return failed: id={} error={}", returnRequest.getId(), ex.getMessage());
            }
        }
        return count;
    }

    private ReturnRequestResponse approve(ReturnRequest returnRequest, String sellerNote, boolean admin) {
        if (returnRequest.getStatus() != ReturnRequestStatus.REQUESTED
            && returnRequest.getStatus() != ReturnRequestStatus.REJECTED
            && returnRequest.getStatus() != ReturnRequestStatus.DISPUTED) {
            throw new BusinessException("Không thể duyệt yêu cầu ở trạng thái hiện tại", HttpStatus.BAD_REQUEST, "RETURN_NOT_APPROVABLE");
        }
        ReturnRequestStatus previous = returnRequest.getStatus();
        returnRequest.setStatus(ReturnRequestStatus.APPROVED);
        returnRequest.setApprovedAt(Instant.now());
        if (sellerNote != null && !sellerNote.isBlank()) {
            returnRequest.setSellerNote(sellerNote.trim());
        }
        if (admin && returnRequest.getAdminNote() == null) {
            returnRequest.setAdminNote("Admin chấp nhận yêu cầu trả hàng");
        }
        ReturnRequest saved = returnRequestRepository.save(returnRequest);
        publishChanged(saved, previous);
        requestReturnShipment(saved);
        return returnRequestMapper.toResponse(saved);
    }

    private ReturnRequestResponse completeRefund(ReturnRequest returnRequest) {
        Order order = returnRequest.getSubOrder() != null ? returnRequest.getSubOrder().getOrder() : null;
        if (order == null) {
            throw new BusinessException("Order not found", HttpStatus.BAD_REQUEST, "ORDER_NOT_FOUND");
        }

        PaymentStatus paymentStatus = order.getPaymentStatus();
        if (paymentStatus != PaymentStatus.COMPLETED
            && paymentStatus != PaymentStatus.PARTIALLY_REFUNDED
            && paymentStatus != PaymentStatus.REFUNDED) {
            throw new BusinessException("Đơn chưa thanh toán nên không hoàn được", HttpStatus.CONFLICT, "PAYMENT_NOT_REFUNDABLE");
        }

        BigDecimal amount = MoneyUtils.normalizeMoney(returnRequest.getRefundAmount());
        String reason = "RETURN_REQUEST:" + returnRequest.getId();
        orderEventProducer.sendPaymentRefundRequest(RefundRequest.builder()
            .orderCode(order.getOrderCode())
            .orderId(order.getId())
            .customerKeycloakId(order.getCustomerKeycloakId())
            .amount(amount)
            .currency(returnRequest.getCurrency() == null ? "VND" : returnRequest.getCurrency())
            .reason(reason)
            .requestedAt(Instant.now())
            .build());

        Instant now = Instant.now();
        ReturnRequestStatus previous = returnRequest.getStatus();
        returnRequest.setStatus(ReturnRequestStatus.REFUNDED);
        returnRequest.setReceivedAt(now);
        returnRequest.setRefundedAt(now);

        SubOrder subOrder = returnRequest.getSubOrder();
        Set<Long> returnedItemIds = Optional.ofNullable(returnRequest.getItems()).orElseGet(List::of).stream()
            .map(ReturnRequestItem::getOrderItemId)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());

        for (OrderItem item : Optional.ofNullable(subOrder.getItems()).orElseGet(List::of)) {
            if (item == null || !returnedItemIds.contains(item.getId()) || item.getStatus() == OrderItemStatus.RETURNED) {
                continue;
            }
            item.setStatus(OrderItemStatus.RETURNED);
            if (item.getProductId() != null && item.getQuantity() != null) {
                orderEventProducer.sendStockRollbackRequest(StockRollbackRequest.builder()
                    .orderId(order.getOrderCode())
                    .productId(item.getProductId())
                    .quantity(item.getQuantity())
                    .timestamp(LocalDateTime.now())
                    .build());
            }
        }

        boolean allReturnedOrCancelled = Optional.ofNullable(subOrder.getItems()).orElseGet(List::of).stream()
            .filter(Objects::nonNull)
            .allMatch(item -> item.getStatus() == OrderItemStatus.RETURNED || item.getStatus() == OrderItemStatus.CANCELLED);
        if (allReturnedOrCancelled) {
            subOrder.setStatus(SubOrderStatus.RETURNED);
            if (subOrder.getPayoutStatus() == PayoutStatus.PENDING) {
                subOrder.setPayoutStatus(PayoutStatus.CANCELLED);
            }
        }

        boolean anyActive = Optional.ofNullable(order.getSubOrders()).orElseGet(List::of).stream()
            .filter(so -> so.getStatus() != SubOrderStatus.CANCELLED && so.getStatus() != SubOrderStatus.RETURNED)
            .findAny()
            .isPresent();
        order.setPaymentStatus(anyActive ? PaymentStatus.PARTIALLY_REFUNDED : PaymentStatus.REFUNDED);
        orderRepository.save(order);

        ReturnRequest saved = returnRequestRepository.save(returnRequest);
        publishChanged(saved, previous);
        return returnRequestMapper.toResponse(saved);
    }

    @Transactional
    public ReturnRequestResponse applyCarrierEvent(
        Long returnRequestId,
        Long subOrderId,
        Long shipmentId,
        String externalShipmentId,
        String carrierCode,
        String shipmentStatus
    ) {
        ReturnRequest returnRequest = resolveReturnForCarrier(returnRequestId, subOrderId, shipmentId);
        if (returnRequest == null) {
            log.warn("No return request for carrier event: returnId={} subOrderId={} shipmentId={}", returnRequestId, subOrderId, shipmentId);
            return null;
        }
        if (returnRequest.getStatus() != null && returnRequest.getStatus().isTerminal()) {
            return returnRequestMapper.toResponse(returnRequest);
        }

        attachCarrierShipment(returnRequest, shipmentId, externalShipmentId, carrierCode);
        String status = shipmentStatus == null ? "" : shipmentStatus.trim().toUpperCase();

        if ("PICKED_UP".equals(status) || "IN_TRANSIT".equals(status)) {
            if (returnRequest.getStatus() == ReturnRequestStatus.APPROVED) {
                ReturnRequestStatus previous = returnRequest.getStatus();
                returnRequest.setStatus(ReturnRequestStatus.SHIPPED);
                returnRequest.setShippedAt(Instant.now());
                ReturnRequest saved = returnRequestRepository.save(returnRequest);
                publishChanged(saved, previous);
                return returnRequestMapper.toResponse(saved);
            }
            return returnRequestMapper.toResponse(returnRequestRepository.save(returnRequest));
        }

        if ("DELIVERED".equals(status)) {
            if (returnRequest.getStatus() == ReturnRequestStatus.APPROVED) {
                returnRequest.setStatus(ReturnRequestStatus.SHIPPED);
                returnRequest.setShippedAt(Instant.now());
            }
            if (returnRequest.getStatus() == ReturnRequestStatus.SHIPPED
                || returnRequest.getStatus() == ReturnRequestStatus.APPROVED) {
                return completeRefund(returnRequest);
            }
        }

        return returnRequestMapper.toResponse(returnRequestRepository.save(returnRequest));
    }

    private ReturnRequest resolveReturnForCarrier(Long returnRequestId, Long subOrderId, Long shipmentId) {
        if (returnRequestId != null) {
            Optional<ReturnRequest> byId = returnRequestRepository.findDetailById(returnRequestId);
            if (byId.isPresent()) {
                return byId.get();
            }
        }
        if (shipmentId != null) {
            Optional<ReturnRequest> byShipment = returnRequestRepository.findByReturnShipmentId(shipmentId);
            if (byShipment.isPresent()) {
                return returnRequestRepository.findDetailById(byShipment.get().getId()).orElse(byShipment.get());
            }
        }
        if (subOrderId != null) {
            return returnRequestRepository
                .findFirstBySubOrder_IdAndStatusInOrderByCreatedAtDesc(subOrderId, ReturnRequestStatus.OPEN)
                .map(found -> returnRequestRepository.findDetailById(found.getId()).orElse(found))
                .orElse(null);
        }
        return null;
    }

    private void attachCarrierShipment(ReturnRequest returnRequest, Long shipmentId, String externalShipmentId, String carrierCode) {
        if (shipmentId != null) {
            returnRequest.setReturnShipmentId(shipmentId);
        }
        if (externalShipmentId != null && !externalShipmentId.isBlank()) {
            returnRequest.setReturnExternalShipmentId(externalShipmentId.trim());
        }
        if (carrierCode != null && !carrierCode.isBlank()) {
            returnRequest.setReturnCarrierCode(carrierCode.trim());
        }
    }

    private void requestReturnShipment(ReturnRequest returnRequest) {
        SubOrder subOrder = returnRequest.getSubOrder();
        Order order = subOrder != null ? subOrder.getOrder() : null;
        if (subOrder == null || order == null || returnRequest.getId() == null) {
            log.warn("Skip return shipment request: missing subOrder/order for returnId={}", returnRequest.getId());
            return;
        }
        String pickup = order.getShippingAddress();
        if (pickup == null || pickup.isBlank()) {
            log.warn("Skip return shipment request: missing customer address for returnId={}", returnRequest.getId());
            return;
        }
        SubOrderShippingRequested request = SubOrderShippingRequested.builder()
            .eventId(UUID.randomUUID().toString())
            .correlationId("return-" + returnRequest.getId())
            .subOrderId(subOrder.getId())
            .orderId(order.getId())
            .vendorKeycloakId(returnRequest.getVendorKeycloakId())
            .pickupAddress(pickup)
            .deliveryAddress(null)
            .serviceLevel("STANDARD")
            .carrierPreference(null)
            .shipmentType("RETURN")
            .returnRequestId(returnRequest.getId())
            .createdAt(Instant.now())
            .build();
        orderEventProducer.sendSubOrderShippingRequested(request);
    }

    private ReturnRequest requireReturn(Long returnId) {
        return returnRequestRepository.findDetailById(returnId)
            .orElseThrow(() -> new ResourceNotFoundException("Return request not found"));
    }

    private ReturnRequest requireVendorReturn(String vendorKeycloakId, Long returnId) {
        ReturnRequest returnRequest = requireReturn(returnId);
        if (!vendorKeycloakId.equals(returnRequest.getVendorKeycloakId())) {
            throw new ResourceNotFoundException("Return request not found");
        }
        return returnRequest;
    }

    private ReturnRequest requireCustomerReturn(String customerKeycloakId, String orderCode, Long returnId) {
        ReturnRequest returnRequest = requireReturn(returnId);
        if (!customerKeycloakId.equals(returnRequest.getCustomerKeycloakId())
            || !orderCode.equals(returnRequest.getOrderCode())) {
            throw new ResourceNotFoundException("Return request not found");
        }
        return returnRequest;
    }

    private void publishChanged(ReturnRequest returnRequest, ReturnRequestStatus previous) {
        Long orderId = returnRequest.getSubOrder() != null && returnRequest.getSubOrder().getOrder() != null
            ? returnRequest.getSubOrder().getOrder().getId()
            : null;
        Long subOrderId = returnRequest.getSubOrder() != null ? returnRequest.getSubOrder().getId() : null;
        orderEventProducer.sendReturnRequestChanged(ReturnRequestChanged.builder()
            .eventId(UUID.randomUUID().toString())
            .returnRequestId(returnRequest.getId())
            .orderCode(returnRequest.getOrderCode())
            .orderId(orderId)
            .subOrderId(subOrderId)
            .customerKeycloakId(returnRequest.getCustomerKeycloakId())
            .vendorKeycloakId(returnRequest.getVendorKeycloakId())
            .status(returnRequest.getStatus())
            .previousStatus(previous)
            .refundAmount(returnRequest.getRefundAmount())
            .reason(returnRequest.getReason() != null ? returnRequest.getReason().name() : null)
            .changedAt(Instant.now())
            .build());
    }

    private Pageable pageable(int page, int size) {
        int safePage = Math.max(0, page);
        int safeSize = Math.min(Math.max(1, size), 50);
        return PageRequest.of(safePage, safeSize, Sort.by(Sort.Direction.DESC, "createdAt"));
    }

    private PageResponse<ReturnRequestResponse> toPage(Page<ReturnRequest> page) {
        return PageResponse.<ReturnRequestResponse>builder()
            .items(page.getContent().stream().map(returnRequestMapper::toResponse).toList())
            .page(page.getNumber())
            .size(page.getSize())
            .totalElements(page.getTotalElements())
            .totalPages(page.getTotalPages())
            .last(page.isLast())
            .build();
    }

    private String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
