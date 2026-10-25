package com.tlcn.order_service.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class ReturnAutoApproveJob {

    private final ReturnRequestService returnRequestService;

    @Scheduled(fixedDelayString = "${return.auto-approve-scan-ms:300000}")
    public void autoApproveExpired() {
        int approved = returnRequestService.autoApproveExpired();
        if (approved > 0) {
            log.info("Auto-approved {} expired return requests", approved);
        }
    }
}
