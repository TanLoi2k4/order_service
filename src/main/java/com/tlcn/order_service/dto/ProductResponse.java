package com.tlcn.order_service.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.constraints.*;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProductResponse {

    @NotNull(message = "Product ID is required")
    private Long id;

    @NotBlank(message = "Product name is required")
    @Size(min = 1, max = 100, message = "Product name must be between 1 and 100 characters")
    private String name;

    @Size(max = 500, message = "Description must not exceed 500 characters")
    private String description;

    private CategoryInfo category;

    @NotNull(message = "Price is required")
    @Min(value = 0, message = "Price must be non-negative")
    private Double price;

    @NotNull(message = "Stock is required")
    @Min(value = 0, message = "Stock must be non-negative")
    private Integer stock;

    @NotBlank(message = "Vendor Keycloak ID is required")
    private String keycloakId;

    @Size(max = 255, message = "Image URL must not exceed 255 characters")
    private String imageUrl;

    public String getCategoryName() {
        return category != null ? category.getName() : null;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CategoryInfo {
        private Long id;
        private String name;
    }
}
