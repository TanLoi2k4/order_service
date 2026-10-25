package com.tlcn.order_service.controller;

import com.tlcn.order_service.dto.CoPurchaseCountDto;
import com.tlcn.order_service.dto.ProductSalesAggregateDTO;
import com.tlcn.order_service.dto.ResponseDTO;
import com.tlcn.order_service.exception.ResourceNotFoundException;
import com.tlcn.order_service.model.SubOrder;
import com.tlcn.order_service.repository.SubOrderRepository;
import com.tlcn.order_service.service.CustomerOrderService;
import com.tlcn.order_service.service.VendorOrderService;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.List;

@RestController
@RequestMapping("/orders/internal")
@RequiredArgsConstructor
public class OrderInternalController {

    private final SubOrderRepository subOrderRepository;
    private final VendorOrderService vendorOrderService;
    private final CustomerOrderService customerOrderService;

    @GetMapping("/sub-orders/{subOrderId}/customer-keycloak-id")
    public ResponseEntity<ResponseDTO<String>> getCustomerKeycloakId(@PathVariable Long subOrderId) {
        SubOrder subOrder = subOrderRepository.findByIdWithOrder(subOrderId)
                .orElseThrow(() -> new ResourceNotFoundException("SubOrder not found: " + subOrderId));
        String customerId = subOrder.getOrder().getCustomerKeycloakId();
        return ResponseEntity.ok(new ResponseDTO<>(true, "OK", customerId));
    }

    @GetMapping("/sub-orders/{subOrderId}/vendor-keycloak-id")
    public ResponseEntity<ResponseDTO<String>> getVendorKeycloakId(@PathVariable Long subOrderId) {
        SubOrder subOrder = subOrderRepository.findByIdWithOrder(subOrderId)
                .orElseThrow(() -> new ResourceNotFoundException("SubOrder not found: " + subOrderId));
        return ResponseEntity.ok(new ResponseDTO<>(true, "OK", subOrder.getVendorKeycloakId()));
    }

    @GetMapping("/vendors/{vendorKeycloakId}/product-sales")
    public ResponseEntity<ResponseDTO<List<ProductSalesAggregateDTO>>> getVendorProductSales(
        @PathVariable String vendorKeycloakId,
        @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate from,
        @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate to,
        @RequestParam(defaultValue = "10") int limit
    ) {
        List<ProductSalesAggregateDTO> data = vendorOrderService.getCompletedProductSales(
            vendorKeycloakId,
            from,
            to,
            limit
        );
        return ResponseEntity.ok(new ResponseDTO<>(true, "OK", data));
    }

    @GetMapping("/product-sales")
    public ResponseEntity<ResponseDTO<List<ProductSalesAggregateDTO>>> getPlatformProductSales(
        @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate from,
        @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate to,
        @RequestParam(defaultValue = "20") int limit
    ) {
        List<ProductSalesAggregateDTO> data = vendorOrderService.getCompletedPlatformProductSales(from, to, limit);
        return ResponseEntity.ok(new ResponseDTO<>(true, "OK", data));
    }

    @GetMapping("/customers/{customerKeycloakId}/purchased-product-ids")
    public ResponseEntity<ResponseDTO<List<Long>>> getPurchasedProductIds(
            @PathVariable String customerKeycloakId
    ) {
        return ResponseEntity.ok(new ResponseDTO<>(
                true,
                "OK",
                customerOrderService.getPurchasedProductIds(customerKeycloakId)
        ));
    }

    @GetMapping("/co-purchases")
    public ResponseEntity<ResponseDTO<List<CoPurchaseCountDto>>> getCoPurchases(
            @RequestParam(name = "productIds") List<Long> productIds
    ) {
        return ResponseEntity.ok(new ResponseDTO<>(
                true,
                "OK",
                customerOrderService.getCoPurchases(productIds)
        ));
    }

    @GetMapping("/product-sold-counts")
    public ResponseEntity<ResponseDTO<List<ProductSalesAggregateDTO>>> getLifetimeProductSold() {
        return ResponseEntity.ok(new ResponseDTO<>(true, "OK", vendorOrderService.getLifetimeProductSold()));
    }
}
