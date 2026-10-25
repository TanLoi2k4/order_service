package com.tlcn.order_service.dto;

import com.tlcn.order_service.enums.ReturnReason;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Data;

import java.util.List;

@Data
public class CreateReturnRequest {

    @NotEmpty(message = "At least one item is required")
    private List<Long> itemIds;

    @NotNull(message = "Return reason is required")
    private ReturnReason reason;

    @Size(max = 1000)
    private String description;
}
