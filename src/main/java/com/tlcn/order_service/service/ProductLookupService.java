package com.tlcn.order_service.service;

import com.tlcn.order_service.client.ProductClient;
import com.tlcn.order_service.dto.ProductResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

/**
 * Public cacheable facade — Spring AOP can intercept this bean (unlike private methods).
 */
@Service
@RequiredArgsConstructor
public class ProductLookupService {

    private final ProductClient productClient;

    @Cacheable(value = "products", key = "#productId", unless = "#result == null")
    public ProductResponse getProduct(Long productId) {
        return productClient.getProduct(productId);
    }
}
