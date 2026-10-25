package com.tlcn.order_service.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.HttpStatusEntryPoint;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;

@Configuration
@EnableWebSecurity
@EnableMethodSecurity
public class SecurityConfig {

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        // 1 Cấu hình JwtAuthenticationConverter với KeycloakConverter
        JwtAuthenticationConverter jwtAuthConverter = new JwtAuthenticationConverter();
        jwtAuthConverter.setJwtGrantedAuthoritiesConverter(new KeycloakConverter());

        http
            // 2 Tắt CSRF (REST API không cần)
            .csrf(csrf -> csrf.disable())

            // 3 Cấu hình quyền truy cập (Authorization rules)
            .authorizeHttpRequests(auth -> auth
                // Public endpoints (Swagger, Actuator, tài liệu API)
                .requestMatchers(
                    "/actuator/**"
                ).permitAll()

                .requestMatchers("/orders/internal/**").permitAll()

                // Admin order endpoints — ADMIN and SUPER_ADMIN (match gateway + @PreAuthorize)
                .requestMatchers("/orders/admin/**").hasAnyRole("ADMIN", "SUPER_ADMIN")

                // Các endpoint giỏ hàng — chỉ CUSTOMER mới được truy cập
                .requestMatchers("/cart/**").hasRole("CUSTOMER")

                // Các endpoint order — CUSTOMER và VENDOR đều có quyền
                .requestMatchers("/orders/**").hasAnyRole("CUSTOMER", "VENDOR")

                // Mọi request khác đều yêu cầu xác thực
                .anyRequest().authenticated()
            )

            // 4 Cấu hình OAuth2 Resource Server (dùng JWT từ Keycloak)
            .oauth2ResourceServer(oauth2 -> oauth2
                .jwt(jwt -> jwt.jwtAuthenticationConverter(jwtAuthConverter))
            )

            // 5 Cấu hình session là stateless (chuẩn REST)
            .sessionManagement(session -> session
                .sessionCreationPolicy(SessionCreationPolicy.STATELESS)
            )

            // 6 Exception handling — trả HTTP 401 thay vì redirect
            .exceptionHandling(exceptions -> exceptions
                .authenticationEntryPoint(new HttpStatusEntryPoint(HttpStatus.UNAUTHORIZED))
            )

            // 7 Thêm filter chặn token trong blacklist (logout)
            .addFilterBefore(new JwtBlacklistFilter(redisTemplate), BasicAuthenticationFilter.class);

        return http.build();
    }
}