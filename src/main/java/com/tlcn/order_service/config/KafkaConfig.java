package com.tlcn.order_service.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id:order-service-group}")
    private String groupId;

    @Value("${spring.kafka.consumer.properties.spring.json.trusted.packages:java.util.*,com.tlcn.*}")
    private String trustedPackages;

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // Stable alias type ids for cross-service DTO compatibility
        config.put(JsonSerializer.TYPE_MAPPINGS,
            "stockReservationRequest:com.tlcn.order_service.dto.kafka.StockReservationRequest,"
                + "stockRollbackRequest:com.tlcn.order_service.dto.kafka.StockRollbackRequest,"
                + "subOrderPayoutRequested:com.tlcn.order_service.dto.kafka.SubOrderPayoutRequested,"
                + "stockReservationResponse:com.tlcn.order_service.dto.kafka.StockReservationResponse,"
                + "discountCommand:com.tlcn.order_service.dto.kafka.DiscountCommand,"
                + "discountEvent:com.tlcn.order_service.dto.kafka.DiscountEvent,"
                + "paymentSuccess:com.tlcn.order_service.dto.kafka.PaymentSuccess,"
                + "refundRequest:com.tlcn.order_service.dto.RefundRequest,"
                + "orderCompleted:com.tlcn.order_service.dto.kafka.OrderCompleted,"
                + "returnRequestChanged:com.tlcn.order_service.dto.kafka.ReturnRequestChanged"
        );

        return new DefaultKafkaProducerFactory<>(
            config,
            new StringSerializer(),
            new JsonSerializer<>(objectMapper())
        );
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        config.put(JsonDeserializer.TRUSTED_PACKAGES, trustedPackages);

        config.put(JsonDeserializer.TYPE_MAPPINGS,
            "stockReservationRequest:com.tlcn.order_service.dto.kafka.StockReservationRequest,"
                + "stockRollbackRequest:com.tlcn.order_service.dto.kafka.StockRollbackRequest,"
                + "subOrderPayoutRequested:com.tlcn.order_service.dto.kafka.SubOrderPayoutRequested,"
                + "stockReservationResponse:com.tlcn.order_service.dto.kafka.StockReservationResponse,"
                + "discountCommand:com.tlcn.order_service.dto.kafka.DiscountCommand,"
                + "discountEvent:com.tlcn.order_service.dto.kafka.DiscountEvent,"
                + "paymentSuccess:com.tlcn.order_service.dto.kafka.PaymentSuccess,"
                + "paymentFailed:com.tlcn.order_service.dto.kafka.PaymentFailed,"
                + "refundRequest:com.tlcn.order_service.dto.RefundRequest,"
                + "shipmentCreated:com.tlcn.order_service.dto.kafka.ShipmentCreated,"
                + "shipmentStatusChanged:com.tlcn.order_service.dto.kafka.ShipmentStatusChanged,"
                + "com.tlcn.shipment_service.dto.kafka.ShipmentCreated:com.tlcn.order_service.dto.kafka.ShipmentCreated,"
                + "com.tlcn.shipment_service.dto.kafka.ShipmentStatusChanged:com.tlcn.order_service.dto.kafka.ShipmentStatusChanged"
        );
        config.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, true);

        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        JsonDeserializer<Object> jsonDeserializer = new JsonDeserializer<>(Object.class, objectMapper());

        return new DefaultKafkaConsumerFactory<>(
            config,
            new StringDeserializer(),
            new ErrorHandlingDeserializer<>(jsonDeserializer)
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setBatchListener(false);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        return mapper;
    }
}
