package com.tlcn.order_service.repository;

import com.tlcn.order_service.enums.ReturnRequestStatus;
import com.tlcn.order_service.model.ReturnRequest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface ReturnRequestRepository extends JpaRepository<ReturnRequest, Long> {

    @EntityGraph(attributePaths = {"items", "subOrder"})
    Optional<ReturnRequest> findWithItemsById(Long id);

    @Query("""
        SELECT DISTINCT r FROM ReturnRequest r
        LEFT JOIN FETCH r.items
        LEFT JOIN FETCH r.subOrder so
        LEFT JOIN FETCH so.order
        WHERE r.id = :id
        """)
    Optional<ReturnRequest> findDetailById(@Param("id") Long id);

    boolean existsBySubOrder_IdAndStatusIn(Long subOrderId, Collection<ReturnRequestStatus> statuses);

    @EntityGraph(attributePaths = "items")
    List<ReturnRequest> findBySubOrder_IdInOrderByCreatedAtDesc(Collection<Long> subOrderIds);

    @EntityGraph(attributePaths = "items")
    List<ReturnRequest> findByOrderCodeAndCustomerKeycloakIdOrderByCreatedAtDesc(
        String orderCode,
        String customerKeycloakId
    );

    @EntityGraph(attributePaths = {"items", "subOrder"})
    Page<ReturnRequest> findByVendorKeycloakId(String vendorKeycloakId, Pageable pageable);

    @EntityGraph(attributePaths = {"items", "subOrder"})
    Page<ReturnRequest> findByVendorKeycloakIdAndStatus(
        String vendorKeycloakId,
        ReturnRequestStatus status,
        Pageable pageable
    );

    @EntityGraph(attributePaths = {"items", "subOrder"})
    Page<ReturnRequest> findByStatus(ReturnRequestStatus status, Pageable pageable);

    @EntityGraph(attributePaths = {"items", "subOrder"})
    Page<ReturnRequest> findAllByOrderByCreatedAtDesc(Pageable pageable);

    List<ReturnRequest> findByStatusAndCreatedAtBefore(ReturnRequestStatus status, Instant cutoff);

    Optional<ReturnRequest> findByReturnShipmentId(Long returnShipmentId);

    @EntityGraph(attributePaths = {"items", "subOrder"})
    Optional<ReturnRequest> findFirstBySubOrder_IdAndStatusInOrderByCreatedAtDesc(
        Long subOrderId,
        Collection<ReturnRequestStatus> statuses
    );
}
