package com.tlcn.order_service.repository;

import com.tlcn.order_service.model.Cart;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface CartRepository extends JpaRepository<Cart, Long> {
    Optional<Cart> findByKeycloakId(String keycloakId);

    /**
     * Find cart by keycloakId with pessimistic write lock to prevent concurrent modifications.
     * Returns locked row so only one transaction can modify it at a time.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT c FROM Cart c WHERE c.keycloakId = :keycloakId")
    Optional<Cart> findByKeycloakIdWithLock(@Param("keycloakId") String keycloakId);
}