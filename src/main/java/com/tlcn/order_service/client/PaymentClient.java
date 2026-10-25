package com.tlcn.order_service.client;

import com.tlcn.order_service.dto.CreatePaymentRequest;
import com.tlcn.order_service.dto.CreatePaymentResponse;
import com.tlcn.order_service.dto.ResponseDTO;
import com.tlcn.order_service.dto.UpdatePaymentRequest;
import com.tlcn.order_service.exception.BusinessException;
import com.tlcn.order_service.model.Order;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentClient {

    private final RestTemplate restTemplate;

    @CircuitBreaker(name = "paymentService", fallbackMethod = "createPaymentForOrderFallback")
    @Retry(name = "paymentService")
    public CreatePaymentResponse createPaymentForOrder(Order order, String authorizationHeader) {
        CreatePaymentRequest paymentRequest = new CreatePaymentRequest();
        paymentRequest.setOrderCode(order.getOrderCode());
        paymentRequest.setOrderId(order.getId());
        paymentRequest.setCustomerKeycloakId(order.getCustomerKeycloakId());
        paymentRequest.setAmount(order.getTotalPayableAmount());
        paymentRequest.setPaymentMethod(order.getPaymentMethod());

        HttpHeaders headers = new HttpHeaders();
        if (authorizationHeader != null && !authorizationHeader.isBlank()) {
            headers.set(HttpHeaders.AUTHORIZATION, authorizationHeader);
        }
        headers.set("Idempotency-Key", order.getOrderCode());

        HttpEntity<CreatePaymentRequest> entity = new HttpEntity<>(paymentRequest, headers);
        String paymentCreatePath = "http://payment-service/payments/customers/create";

        try {
            ResponseEntity<ResponseDTO<CreatePaymentResponse>> response = restTemplate.exchange(
                paymentCreatePath,
                HttpMethod.POST,
                entity,
                new ParameterizedTypeReference<ResponseDTO<CreatePaymentResponse>>() {}
            );

            ResponseDTO<CreatePaymentResponse> body = response.getBody();
            if (body == null || !body.isSuccess() || body.getData() == null) {
                log.error("Payment service returned unsuccessful response: status={}, body={}", response.getStatusCode(), body);
                throw new BusinessException("Failed to create payment", HttpStatus.BAD_GATEWAY, "PAYMENT_CREATE_FAILED");
            }

            return body.getData();
        } catch (HttpStatusCodeException ex) {
            log.error("Payment service HTTP error: status={}, body={}", ex.getStatusCode(), ex.getResponseBodyAsString(), ex);
            throw new BusinessException("Failed to create payment", HttpStatus.BAD_GATEWAY, "PAYMENT_CREATE_FAILED");
        }
        // RestClientException (timeout/network) propagates so CB/Retry can record it; fallback maps to BusinessException
    }

    @SuppressWarnings("unused")
    private CreatePaymentResponse createPaymentForOrderFallback(Order order, String authorizationHeader, Throwable ex) {
        log.error("Payment circuit/fallback for createPayment orderCode={}: {}",
            order != null ? order.getOrderCode() : null, ex != null ? ex.getMessage() : null);
        if (ex instanceof BusinessException businessException) {
            throw businessException;
        }
        throw new BusinessException("Payment service unavailable", HttpStatus.BAD_GATEWAY, "PAYMENT_SERVICE_UNAVAILABLE");
    }

    @CircuitBreaker(name = "paymentService", fallbackMethod = "updatePendingPaymentFallback")
    @Retry(name = "paymentService")
    public void updatePendingPayment(String orderCode, String authorizationHeader, UpdatePaymentRequest request, String idempotencyKey) {
        UpdatePaymentRequest paymentRequest = new UpdatePaymentRequest();
        paymentRequest.setAmount(normalizeMoney(request.getAmount()));
        paymentRequest.setCurrency(request.getCurrency() == null || request.getCurrency().isBlank() ? "VND" : request.getCurrency());

        HttpEntity<UpdatePaymentRequest> entity = new HttpEntity<>(paymentRequest, buildHeaders(authorizationHeader, idempotencyKey));

        try {
            ResponseEntity<ResponseDTO<Map<String, Object>>> response = restTemplate.exchange(
                "http://payment-service/payments/customers/" + orderCode,
                HttpMethod.PUT,
                entity,
                new ParameterizedTypeReference<ResponseDTO<Map<String, Object>>>() {}
            );

            ResponseDTO<Map<String, Object>> body = response.getBody();
            if (body == null || !body.isSuccess() || body.getData() == null) {
                throw new BusinessException("Failed to update payment", HttpStatus.BAD_GATEWAY, "PAYMENT_UPDATE_FAILED");
            }
        } catch (HttpStatusCodeException ex) {
            throw new BusinessException("Failed to update payment", HttpStatus.BAD_GATEWAY, "PAYMENT_UPDATE_FAILED");
        }
    }

    @SuppressWarnings("unused")
    private void updatePendingPaymentFallback(String orderCode, String authorizationHeader, UpdatePaymentRequest request, String idempotencyKey, Throwable ex) {
        throwPaymentUnavailable(ex, "PAYMENT_UPDATE_FAILED", "Failed to update payment");
    }

    @CircuitBreaker(name = "paymentService", fallbackMethod = "cancelPendingPaymentFallback")
    @Retry(name = "paymentService")
    public void cancelPendingPayment(String orderCode, String authorizationHeader, String idempotencyKey) {
        HttpEntity<Void> entity = new HttpEntity<>(buildHeaders(authorizationHeader, idempotencyKey));

        try {
            ResponseEntity<ResponseDTO<Map<String, Object>>> response = restTemplate.exchange(
                "http://payment-service/payments/customers/" + orderCode + "/cancel",
                HttpMethod.POST,
                entity,
                new ParameterizedTypeReference<ResponseDTO<Map<String, Object>>>() {}
            );

            ResponseDTO<Map<String, Object>> body = response.getBody();
            if (body == null || !body.isSuccess() || body.getData() == null) {
                throw new BusinessException("Failed to cancel payment", HttpStatus.BAD_GATEWAY, "PAYMENT_CANCEL_FAILED");
            }
        } catch (HttpStatusCodeException ex) {
            throw new BusinessException("Failed to cancel payment", HttpStatus.BAD_GATEWAY, "PAYMENT_CANCEL_FAILED");
        }
    }

    @SuppressWarnings("unused")
    private void cancelPendingPaymentFallback(String orderCode, String authorizationHeader, String idempotencyKey, Throwable ex) {
        throwPaymentUnavailable(ex, "PAYMENT_CANCEL_FAILED", "Failed to cancel payment");
    }

    @CircuitBreaker(name = "paymentService", fallbackMethod = "refundPaymentFallback")
    @Retry(name = "paymentService")
    public void refundPayment(String orderCode, String authorizationHeader, BigDecimal amount, String reason, String idempotencyKey) {
        Map<String, Object> refundRequest = new LinkedHashMap<>();
        refundRequest.put("amount", normalizeMoney(amount));
        refundRequest.put("currency", "VND");
        refundRequest.put("reason", reason);

        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(refundRequest, buildHeaders(authorizationHeader, idempotencyKey));

        try {
            ResponseEntity<ResponseDTO<Map<String, Object>>> response = restTemplate.exchange(
                "http://payment-service/payments/customers/" + orderCode + "/refund",
                HttpMethod.POST,
                entity,
                new ParameterizedTypeReference<ResponseDTO<Map<String, Object>>>() {}
            );

            ResponseDTO<Map<String, Object>> body = response.getBody();
            if (body == null || !body.isSuccess() || body.getData() == null) {
                throw new BusinessException("Failed to refund payment", HttpStatus.BAD_GATEWAY, "PAYMENT_REFUND_FAILED");
            }
        } catch (HttpStatusCodeException ex) {
            throw new BusinessException("Failed to refund payment", HttpStatus.BAD_GATEWAY, "PAYMENT_REFUND_FAILED");
        }
    }

    @SuppressWarnings("unused")
    private void refundPaymentFallback(String orderCode, String authorizationHeader, BigDecimal amount, String reason, String idempotencyKey, Throwable ex) {
        throwPaymentUnavailable(ex, "PAYMENT_REFUND_FAILED", "Failed to refund payment");
    }

    @CircuitBreaker(name = "paymentService", fallbackMethod = "updateVendorPaymentFallback")
    @Retry(name = "paymentService")
    public void updateVendorPayment(String orderCode, String authorizationHeader, BigDecimal amount) {
        Map<String, Object> paymentRequest = new LinkedHashMap<>();
        paymentRequest.put("amount", normalizeMoney(amount));
        paymentRequest.put("currency", "VND");

        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(paymentRequest, buildHeaders(authorizationHeader, orderCode));

        try {
            ResponseEntity<ResponseDTO<Map<String, Object>>> response = restTemplate.exchange(
                "http://payment-service/payments/vendor/" + orderCode,
                HttpMethod.PUT,
                entity,
                new ParameterizedTypeReference<ResponseDTO<Map<String, Object>>>() {}
            );

            ResponseDTO<Map<String, Object>> body = response.getBody();
            if (body == null || !body.isSuccess() || body.getData() == null) {
                throw new BusinessException("Failed to update payment", HttpStatus.BAD_GATEWAY, "PAYMENT_UPDATE_FAILED");
            }
        } catch (HttpStatusCodeException ex) {
            throw new BusinessException("Failed to update payment", HttpStatus.BAD_GATEWAY, "PAYMENT_UPDATE_FAILED");
        }
    }

    @SuppressWarnings("unused")
    private void updateVendorPaymentFallback(String orderCode, String authorizationHeader, BigDecimal amount, Throwable ex) {
        throwPaymentUnavailable(ex, "PAYMENT_UPDATE_FAILED", "Failed to update payment");
    }

    @CircuitBreaker(name = "paymentService", fallbackMethod = "cancelVendorPaymentFallback")
    @Retry(name = "paymentService")
    public void cancelVendorPayment(String orderCode, String authorizationHeader, String idempotencyKey) {
        HttpEntity<Void> entity = new HttpEntity<>(buildHeaders(authorizationHeader, idempotencyKey));

        try {
            ResponseEntity<ResponseDTO<Map<String, Object>>> response = restTemplate.exchange(
                "http://payment-service/payments/vendor/" + orderCode + "/cancel",
                HttpMethod.POST,
                entity,
                new ParameterizedTypeReference<ResponseDTO<Map<String, Object>>>() {}
            );

            ResponseDTO<Map<String, Object>> body = response.getBody();
            if (body == null || !body.isSuccess() || body.getData() == null) {
                throw new BusinessException("Failed to cancel payment", HttpStatus.BAD_GATEWAY, "PAYMENT_CANCEL_FAILED");
            }
        } catch (HttpStatusCodeException ex) {
            throw new BusinessException("Failed to cancel payment", HttpStatus.BAD_GATEWAY, "PAYMENT_CANCEL_FAILED");
        }
    }

    @SuppressWarnings("unused")
    private void cancelVendorPaymentFallback(String orderCode, String authorizationHeader, String idempotencyKey, Throwable ex) {
        throwPaymentUnavailable(ex, "PAYMENT_CANCEL_FAILED", "Failed to cancel payment");
    }

    @CircuitBreaker(name = "paymentService", fallbackMethod = "refundVendorPaymentFallback")
    @Retry(name = "paymentService")
    public void refundVendorPayment(String orderCode, String authorizationHeader, BigDecimal amount, String reason, String idempotencyKey) {
        Map<String, Object> refundRequest = new LinkedHashMap<>();
        refundRequest.put("amount", normalizeMoney(amount));
        refundRequest.put("currency", "VND");
        refundRequest.put("reason", reason);

        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(refundRequest, buildHeaders(authorizationHeader, idempotencyKey));

        try {
            ResponseEntity<ResponseDTO<Map<String, Object>>> response = restTemplate.exchange(
                "http://payment-service/payments/vendor/" + orderCode + "/refund",
                HttpMethod.POST,
                entity,
                new ParameterizedTypeReference<ResponseDTO<Map<String, Object>>>() {}
            );

            ResponseDTO<Map<String, Object>> body = response.getBody();
            if (body == null || !body.isSuccess() || body.getData() == null) {
                throw new BusinessException("Failed to refund payment", HttpStatus.BAD_GATEWAY, "PAYMENT_REFUND_FAILED");
            }
        } catch (HttpStatusCodeException ex) {
            throw new BusinessException("Failed to refund payment", HttpStatus.BAD_GATEWAY, "PAYMENT_REFUND_FAILED");
        }
    }

    @SuppressWarnings("unused")
    private void refundVendorPaymentFallback(String orderCode, String authorizationHeader, BigDecimal amount, String reason, String idempotencyKey, Throwable ex) {
        throwPaymentUnavailable(ex, "PAYMENT_REFUND_FAILED", "Failed to refund payment");
    }

    @CircuitBreaker(name = "paymentService", fallbackMethod = "completeCodPaymentFallback")
    @Retry(name = "paymentService")
    public CreatePaymentResponse completeCodPayment(String orderCode) {
        HttpEntity<Void> entity = new HttpEntity<>(new HttpHeaders());

        try {
            ResponseEntity<ResponseDTO<CreatePaymentResponse>> response = restTemplate.exchange(
                "http://payment-service/payments/internal/" + orderCode + "/complete-cod",
                HttpMethod.POST,
                entity,
                new ParameterizedTypeReference<ResponseDTO<CreatePaymentResponse>>() {}
            );

            ResponseDTO<CreatePaymentResponse> body = response.getBody();
            if (body == null || !body.isSuccess() || body.getData() == null) {
                log.error("Payment service returned unsuccessful COD complete response: status={}, body={}", response.getStatusCode(), body);
                throw new BusinessException("Failed to complete COD payment", HttpStatus.BAD_GATEWAY, "PAYMENT_COD_COMPLETE_FAILED");
            }

            return body.getData();
        } catch (HttpStatusCodeException ex) {
            log.error("Payment service HTTP error on COD complete: status={}, body={}", ex.getStatusCode(), ex.getResponseBodyAsString(), ex);
            throw new BusinessException("Failed to complete COD payment", HttpStatus.BAD_GATEWAY, "PAYMENT_COD_COMPLETE_FAILED");
        }
    }

    @SuppressWarnings("unused")
    private CreatePaymentResponse completeCodPaymentFallback(String orderCode, Throwable ex) {
        log.error("Payment circuit/fallback for completeCod orderCode={}: {}", orderCode, ex != null ? ex.getMessage() : null);
        if (ex instanceof BusinessException businessException) {
            throw businessException;
        }
        throw new BusinessException("Payment service unavailable", HttpStatus.BAD_GATEWAY, "PAYMENT_SERVICE_UNAVAILABLE");
    }

    private void throwPaymentUnavailable(Throwable ex, String errorCode, String message) {
        if (ex instanceof BusinessException businessException) {
            throw businessException;
        }
        throw new BusinessException(message, HttpStatus.BAD_GATEWAY, errorCode);
    }

    private HttpHeaders buildHeaders(String authorizationHeader, String idempotencyKey) {
        HttpHeaders headers = new HttpHeaders();
        if (authorizationHeader != null && !authorizationHeader.isBlank()) {
            headers.set(HttpHeaders.AUTHORIZATION, authorizationHeader);
        }
        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            headers.set("Idempotency-Key", idempotencyKey);
        }
        return headers;
    }

    private BigDecimal normalizeMoney(BigDecimal value) {
        if (value == null) {
            return BigDecimal.ZERO.setScale(2, java.math.RoundingMode.HALF_UP);
        }
        return value.setScale(2, java.math.RoundingMode.HALF_UP);
    }
}
