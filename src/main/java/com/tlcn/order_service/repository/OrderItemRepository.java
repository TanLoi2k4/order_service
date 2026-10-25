package com.tlcn.order_service.repository;

import com.tlcn.order_service.model.OrderItem;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.Collection;
import java.util.List;

@Repository
public interface OrderItemRepository extends JpaRepository<OrderItem, Long> {

    List<OrderItem> findBySubOrder_Id(Long subOrderId);

    List<OrderItem> findBySubOrder_IdAndProductId(Long subOrderId, Long productId);

    @Query("""
        SELECT CASE WHEN COUNT(oi) > 0 THEN true ELSE false END
        FROM OrderItem oi
        JOIN oi.subOrder so
        JOIN so.order o
        WHERE o.customerKeycloakId = :customerKeycloakId
          AND oi.productId = :productId
          AND oi.status <> com.tlcn.order_service.enums.OrderItemStatus.CANCELLED
          AND oi.status <> com.tlcn.order_service.enums.OrderItemStatus.RETURNED
          AND so.status <> com.tlcn.order_service.enums.SubOrderStatus.CANCELLED
          AND (
                o.paymentStatus = com.tlcn.order_service.enums.PaymentStatus.COMPLETED
                OR o.paymentStatus = com.tlcn.order_service.enums.PaymentStatus.PARTIALLY_REFUNDED
                OR so.status = com.tlcn.order_service.enums.SubOrderStatus.COMPLETED
              )
        """)
    boolean existsPurchasedProductByCustomer(
        @Param("customerKeycloakId") String customerKeycloakId,
        @Param("productId") Long productId
    );

    @Query("""
        SELECT o.id
        FROM OrderItem oi
        JOIN oi.subOrder so
        JOIN so.order o
        WHERE o.customerKeycloakId = :customerKeycloakId
          AND oi.productId = :productId
          AND oi.status <> com.tlcn.order_service.enums.OrderItemStatus.CANCELLED
          AND oi.status <> com.tlcn.order_service.enums.OrderItemStatus.RETURNED
          AND so.status <> com.tlcn.order_service.enums.SubOrderStatus.CANCELLED
          AND (
                o.paymentStatus = com.tlcn.order_service.enums.PaymentStatus.COMPLETED
                OR o.paymentStatus = com.tlcn.order_service.enums.PaymentStatus.PARTIALLY_REFUNDED
                OR so.status = com.tlcn.order_service.enums.SubOrderStatus.COMPLETED
              )
        ORDER BY o.createdAt DESC
        """)
    List<Long> findPurchasedOrderIdsByCustomer(
        @Param("customerKeycloakId") String customerKeycloakId,
        @Param("productId") Long productId
    );

    @Query("""
        SELECT DISTINCT oi.productId
        FROM OrderItem oi
        JOIN oi.subOrder so
        JOIN so.order o
        WHERE o.customerKeycloakId = :customerKeycloakId
          AND oi.status <> com.tlcn.order_service.enums.OrderItemStatus.CANCELLED
          AND oi.status <> com.tlcn.order_service.enums.OrderItemStatus.RETURNED
          AND so.status <> com.tlcn.order_service.enums.SubOrderStatus.CANCELLED
          AND (
                o.paymentStatus = com.tlcn.order_service.enums.PaymentStatus.COMPLETED
                OR o.paymentStatus = com.tlcn.order_service.enums.PaymentStatus.PARTIALLY_REFUNDED
                OR so.status = com.tlcn.order_service.enums.SubOrderStatus.COMPLETED
              )
        """)
    List<Long> findPurchasedProductIdsByCustomer(
        @Param("customerKeycloakId") String customerKeycloakId
    );

    @Query("""
        SELECT oi.productId, SUM(oi.quantity), SUM(oi.lineTotal)
        FROM OrderItem oi
        JOIN oi.subOrder so
        WHERE so.vendorKeycloakId = :vendorKeycloakId
          AND so.status = com.tlcn.order_service.enums.SubOrderStatus.COMPLETED
          AND so.completedAt >= :from
          AND so.completedAt < :to
          AND oi.status <> com.tlcn.order_service.enums.OrderItemStatus.CANCELLED
          AND oi.status <> com.tlcn.order_service.enums.OrderItemStatus.RETURNED
        GROUP BY oi.productId
        ORDER BY SUM(oi.lineTotal) DESC
        """)
    List<Object[]> aggregateCompletedProductSales(
        @Param("vendorKeycloakId") String vendorKeycloakId,
        @Param("from") Instant from,
        @Param("to") Instant to,
        Pageable pageable
    );

    @Query("""
        SELECT oi.productId, SUM(oi.quantity), SUM(oi.lineTotal)
        FROM OrderItem oi
        JOIN oi.subOrder so
        WHERE so.status = com.tlcn.order_service.enums.SubOrderStatus.COMPLETED
          AND so.completedAt >= :from
          AND so.completedAt < :to
          AND oi.status <> com.tlcn.order_service.enums.OrderItemStatus.CANCELLED
          AND oi.status <> com.tlcn.order_service.enums.OrderItemStatus.RETURNED
        GROUP BY oi.productId
        ORDER BY SUM(oi.lineTotal) DESC
        """)
    List<Object[]> aggregateCompletedPlatformProductSales(
        @Param("from") Instant from,
        @Param("to") Instant to,
        Pageable pageable
    );

    @Query("""
        SELECT oi.productId, SUM(oi.quantity), SUM(oi.lineTotal)
        FROM OrderItem oi
        JOIN oi.subOrder so
        WHERE so.status <> com.tlcn.order_service.enums.SubOrderStatus.CANCELLED
          AND oi.status <> com.tlcn.order_service.enums.OrderItemStatus.CANCELLED
          AND oi.status <> com.tlcn.order_service.enums.OrderItemStatus.RETURNED
        GROUP BY oi.productId
        ORDER BY SUM(oi.quantity) DESC
        """)
    List<Object[]> aggregateLifetimeProductSold();

    /**
     * Số khách khác nhau đã mua source rồi mới mua target.
     * Cùng điều kiện đơn đã mua: bỏ hủy, hoàn, và đơn chưa thanh toán.
     */
    @Query("""
        SELECT earlier.productId, later.productId, COUNT(DISTINCT oEarlier.customerKeycloakId)
        FROM OrderItem earlier
        JOIN earlier.subOrder soEarlier
        JOIN soEarlier.order oEarlier
        JOIN Order oLater
            ON oLater.customerKeycloakId = oEarlier.customerKeycloakId
            AND oEarlier.createdAt < oLater.createdAt
        JOIN oLater.subOrders soLater
        JOIN soLater.items later
        WHERE earlier.productId IN :sourceProductIds
          AND earlier.productId <> later.productId
          AND earlier.status <> com.tlcn.order_service.enums.OrderItemStatus.CANCELLED
          AND earlier.status <> com.tlcn.order_service.enums.OrderItemStatus.RETURNED
          AND soEarlier.status <> com.tlcn.order_service.enums.SubOrderStatus.CANCELLED
          AND (
                oEarlier.paymentStatus = com.tlcn.order_service.enums.PaymentStatus.COMPLETED
                OR oEarlier.paymentStatus = com.tlcn.order_service.enums.PaymentStatus.PARTIALLY_REFUNDED
                OR soEarlier.status = com.tlcn.order_service.enums.SubOrderStatus.COMPLETED
              )
          AND later.status <> com.tlcn.order_service.enums.OrderItemStatus.CANCELLED
          AND later.status <> com.tlcn.order_service.enums.OrderItemStatus.RETURNED
          AND soLater.status <> com.tlcn.order_service.enums.SubOrderStatus.CANCELLED
          AND (
                oLater.paymentStatus = com.tlcn.order_service.enums.PaymentStatus.COMPLETED
                OR oLater.paymentStatus = com.tlcn.order_service.enums.PaymentStatus.PARTIALLY_REFUNDED
                OR soLater.status = com.tlcn.order_service.enums.SubOrderStatus.COMPLETED
              )
        GROUP BY earlier.productId, later.productId
        """)
    List<Object[]> countCustomersWhoBoughtThen(
        @Param("sourceProductIds") Collection<Long> sourceProductIds
    );
}
