package com.tlcn.order_service.kafka;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.tlcn.order_service.dto.kafka.DiscountCommand;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@RequiredArgsConstructor
@Slf4j
public class DiscountCommandProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${spring.kafka.topics.discount-commands:discount.commands}")
    private String discountCommandsTopic;

    @Value("${spring.kafka.topics.discount-replies:discount.replies}")
    private String discountRepliesTopic;

    public void send(DiscountCommand command) {
        send(command, null);
    }

    public void send(DiscountCommand command, String replyToTopic) {
        if (command == null) {
            return;
        }

        if (command.getCommandId() == null || command.getCommandId().isBlank()) {
            command.setCommandId(UUID.randomUUID().toString());
        }

        String messageId = command.getCommandId();
        long timestampMs = Instant.now().toEpochMilli();

        String key = command.getOrderId() != null ? String.valueOf(command.getOrderId()) : null;

        ProducerRecord<String, Object> record = new ProducerRecord<>(discountCommandsTopic, key, command);
        record.headers().add("messageId", messageId.getBytes(StandardCharsets.UTF_8));
        record.headers().add("correlationId", messageId.getBytes(StandardCharsets.UTF_8));
        // Use commandId as idempotency key so downstream/reply consumers can dedupe safely.
        record.headers().add("idempotency-key", messageId.getBytes(StandardCharsets.UTF_8));
        record.headers().add("timestamp", String.valueOf(timestampMs).getBytes(StandardCharsets.UTF_8));

        String replyTo = (replyToTopic != null && !replyToTopic.isBlank()) ? replyToTopic : null;
        if (replyTo != null) {
            record.headers().add("replyTo", replyTo.getBytes(StandardCharsets.UTF_8));
        }

        kafkaTemplate.send(record);

        log.info("Sent discount command: messageId={}, type={}, commandId={}, orderId={}, code={}, replyTo={} -> topic={}",
            messageId,
            command.getType(),
            command.getCommandId(),
            command.getOrderId(),
            command.getCode(),
            replyTo,
            discountCommandsTopic
        );
    }

    public String getDiscountRepliesTopic() {
        return discountRepliesTopic;
    }
}
