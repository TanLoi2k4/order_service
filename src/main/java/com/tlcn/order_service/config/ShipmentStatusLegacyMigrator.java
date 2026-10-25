package com.tlcn.order_service.config;

import com.tlcn.order_service.enums.ShipmentStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Hibernate {@code ddl-auto=update} does not refresh PostgreSQL CHECK constraints
 * when enum values change (PENDING/DELIVERING -> BOOKED/IN_TRANSIT). Without this
 * runner, Kafka consumers fail with {@code shipments_status_check} on BOOKED.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
@RequiredArgsConstructor
@Slf4j
public class ShipmentStatusLegacyMigrator implements ApplicationRunner {

    private static final String CONSTRAINT_NAME = "shipments_status_check";

    private final JdbcTemplate jdbcTemplate;
    private final PlatformTransactionManager transactionManager;

    @Override
    public void run(ApplicationArguments args) {
        try {
            Boolean tableExists = jdbcTemplate.queryForObject(
                """
                SELECT EXISTS (
                  SELECT 1
                  FROM information_schema.tables
                  WHERE table_schema = current_schema()
                    AND table_name = 'shipments'
                )
                """,
                Boolean.class
            );
            if (!Boolean.TRUE.equals(tableExists)) {
                return;
            }

            TransactionTemplate tx = new TransactionTemplate(transactionManager);
            tx.executeWithoutResult(status -> {
                jdbcTemplate.execute("ALTER TABLE shipments DROP CONSTRAINT IF EXISTS " + CONSTRAINT_NAME);

                int booked = jdbcTemplate.update("UPDATE shipments SET status = 'BOOKED' WHERE status = 'PENDING'");
                int inTransit = jdbcTemplate.update("UPDATE shipments SET status = 'IN_TRANSIT' WHERE status = 'DELIVERING'");
                if (booked > 0 || inTransit > 0) {
                    log.info("Migrated legacy shipment statuses: PENDING->BOOKED={}, DELIVERING->IN_TRANSIT={}", booked, inTransit);
                }

                jdbcTemplate.execute(
                    "ALTER TABLE shipments ADD CONSTRAINT " + CONSTRAINT_NAME
                        + " CHECK (status IN (" + allowedStatusesSql() + "))"
                );
            });
            log.info("Ensured {} allows current ShipmentStatus values", CONSTRAINT_NAME);
        } catch (Exception e) {
            log.warn("Skip shipment status constraint migration: {}", e.getMessage());
        }
    }

    private String allowedStatusesSql() {
        return Arrays.stream(ShipmentStatus.values())
            .map(value -> "'" + value.name() + "'")
            .collect(Collectors.joining(", "));
    }
}
