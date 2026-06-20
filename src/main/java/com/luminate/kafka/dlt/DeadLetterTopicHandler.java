package com.luminate.kafka.dlt;

import com.luminate.ingestion.dto.LogPayload;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class DeadLetterTopicHandler {

    private final KafkaTemplate<String, LogPayload> kafkaTemplate;

    @Value("${luminate.kafka.topics.dead-letter}")
    private String dltTopic;

    /**
     * Publishes a failed log payload to the Dead Letter Topic.
     * Called after all retry attempts are exhausted.
     */
    public void sendToDlt(LogPayload payload) {
        log.error("Sending to DLT — service: {}, traceId: {}",
                payload.getServiceName(), payload.getTraceId());

        kafkaTemplate.send(dltTopic, payload.getServiceName(), payload)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send to DLT — traceId: {}, error: {}",
                                payload.getTraceId(), ex.getMessage());
                    } else {
                        log.info("Successfully routed to DLT — traceId: {}",
                                payload.getTraceId());
                    }
                });
    }
}