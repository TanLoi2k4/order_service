package com.tlcn.order_service.dto;

import jakarta.validation.constraints.Size;
import lombok.Data;

@Data
public class ReturnActionRequest {

    @Size(max = 1000)
    private String note;
}
