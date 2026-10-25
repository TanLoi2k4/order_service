package com.tlcn.order_service.repository;

import com.tlcn.order_service.enums.PayoutStatus;
import com.tlcn.order_service.enums.SubOrderStatus;
import com.tlcn.order_service.model.SubOrder;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
public interface SubOrderRepository extends JpaRepository<SubOrder, Long> {

    @Query("SELECT so FROM SubOrder so JOIN FETCH so.order WHERE so.id = :id")
    Optional<SubOrder> findByIdWithOrder(@Param("id") Long id);

    @Query("SELECT DISTINCT so FROM SubOrder so JOIN FETCH so.order LEFT JOIN FETCH so.items WHERE so.id = :id")
    Optional<SubOrder> findByIdWithOrderAndItems(@Param("id") Long id);

    @Query("SELECT DISTINCT so FROM SubOrder so LEFT JOIN FETCH so.items WHERE so.order.id = :orderId")
    List<SubOrder> findByOrderIdWithItems(@Param("orderId") Long orderId);

    List<SubOrder> findByOrder_Id(Long orderId);

    List<SubOrder> findByVendorKeycloakId(String vendorKeycloakId);

    Page<SubOrder> findByVendorKeycloakId(String vendorKeycloakId, Pageable pageable);

    @EntityGraph(attributePaths = "order")
    @Query("""
        SELECT so FROM SubOrder so
        WHERE so.vendorKeycloakId = :vendorKeycloakId
          AND (:status IS NULL OR so.status = :status)
          AND (:customerKeycloakId IS NULL OR so.order.customerKeycloakId = :customerKeycloakId)
          AND (
            :q IS NULL
            OR LOWER(so.order.orderCode) LIKE :q
            OR LOWER(COALESCE(so.order.customerName, '')) LIKE :q
            OR LOWER(COALESCE(so.order.customerPhone, '')) LIKE :q
            OR CAST(so.id AS string) LIKE :q
          )
        """)
    Page<SubOrder> findVendorSubOrders(
        @Param("vendorKeycloakId") String vendorKeycloakId,
        @Param("status") SubOrderStatus status,
        @Param("customerKeycloakId") String customerKeycloakId,
        @Param("q") String q,
        Pageable pageable
    );

    @Query("""
        SELECT so.status, COUNT(so)
        FROM SubOrder so
        WHERE so.vendorKeycloakId = :vendorKeycloakId
        GROUP BY so.status
        """)
    List<Object[]> countVendorSubOrdersByStatus(@Param("vendorKeycloakId") String vendorKeycloakId);

    @Query("""
        SELECT so.status, COUNT(so)
        FROM SubOrder so
        WHERE so.vendorKeycloakId = :vendorKeycloakId
          AND so.order.createdAt >= :from
          AND so.order.createdAt < :to
        GROUP BY so.status
        """)
    List<Object[]> countVendorSubOrdersByStatusBetween(
        @Param("vendorKeycloakId") String vendorKeycloakId,
        @Param("from") Instant from,
        @Param("to") Instant to
    );

    Optional<SubOrder> findByIdAndVendorKeycloakId(Long id, String vendorKeycloakId);

    List<SubOrder> findByOrder_IdAndVendorKeycloakId(Long orderId, String vendorKeycloakId);

    List<SubOrder> findByPayoutStatus(PayoutStatus payoutStatus);

    List<SubOrder> findByVendorKeycloakIdAndPayoutStatus(String vendorKeycloakId, PayoutStatus payoutStatus);
}
