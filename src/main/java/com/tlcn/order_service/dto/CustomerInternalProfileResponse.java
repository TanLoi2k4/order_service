package com.tlcn.order_service.dto;

import lombok.Data;

@Data
public class CustomerInternalProfileResponse {
    private String keycloakId;
    private String displayName;
    private String phone;
}
