package com.tlcn.order_service.client;

import com.tlcn.order_service.dto.CustomerInternalProfileResponse;
import com.tlcn.order_service.dto.ResponseDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

@Component
@RequiredArgsConstructor
@Slf4j
public class CustomerClient {

    private final RestTemplate restTemplate;

    public CustomerInternalProfileResponse getProfile(String keycloakId) {
        if (keycloakId == null || keycloakId.isBlank()) {
            return null;
        }
        try {
            ResponseEntity<ResponseDTO<CustomerInternalProfileResponse>> response = restTemplate.exchange(
                "http://customer-service/customers/internal/{keycloakId}",
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<ResponseDTO<CustomerInternalProfileResponse>>() {},
                keycloakId
            );
            ResponseDTO<CustomerInternalProfileResponse> body = response.getBody();
            if (body == null || !body.isSuccess()) {
                return null;
            }
            return body.getData();
        } catch (RestClientException ex) {
            log.warn("Could not fetch customer profile for keycloakId={}: {}", keycloakId, ex.getMessage());
            return null;
        }
    }
}
