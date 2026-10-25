package com.tlcn.order_service.client;

import com.tlcn.order_service.dto.ProductResponse;
import com.tlcn.order_service.dto.ResponseDTO;
import com.tlcn.order_service.exception.BusinessException;
import com.tlcn.order_service.exception.ResourceNotFoundException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

@Component
@RequiredArgsConstructor
@Slf4j
public class ProductClient {

    private final RestTemplate restTemplate;

    @CircuitBreaker(name = "productService", fallbackMethod = "getProductFallback")
    @Retry(name = "productService")
    public ProductResponse getProduct(Long productId) {
        try {
            ResponseEntity<ResponseDTO<ProductResponse>> response = restTemplate.exchange(
                "http://product-service/products/{productId}",
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<ResponseDTO<ProductResponse>>() {},
                productId
            );

            ResponseDTO<ProductResponse> body = response.getBody();
            if (response.getStatusCode().isError() || body == null || !body.isSuccess() || body.getData() == null) {
                throw new ResourceNotFoundException("Failed to fetch product with ID: " + productId);
            }

            return body.getData();
        } catch (HttpClientErrorException e) {
            throw new ResourceNotFoundException(
                "Error fetching product with ID: " + productId + " (HTTP " + e.getStatusCode() + ": " + e.getStatusText() + ")"
            );
        }
        // RestClientException (timeout/network) propagates for CB/Retry; fallback maps it
    }

    @SuppressWarnings("unused")
    private ProductResponse getProductFallback(Long productId, Throwable ex) {
        log.error("Product circuit/fallback for productId={}: {}", productId, ex != null ? ex.getMessage() : null);
        if (ex instanceof ResourceNotFoundException resourceNotFoundException) {
            throw resourceNotFoundException;
        }
        if (ex instanceof BusinessException businessException) {
            throw businessException;
        }
        throw new BusinessException("Product service unavailable", HttpStatus.BAD_GATEWAY, "PRODUCT_SERVICE_UNAVAILABLE");
    }
}
