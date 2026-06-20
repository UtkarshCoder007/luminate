package com.luminate.kafka.producer;

import com.luminate.ingestion.dto.LogPayload;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class LogEventProducer {

    private final KafkaTemplate<String, LogPayload> kafkaTemplate;

    @Value("${luminate.kafka.topics.incoming-logs}")
    private String incomingLogsTopic;

    /**
     * Publishes a batch of logs to Kafka.
     * Each log is keyed by serviceName so logs from the same service
     * always land on the same partition — preserving order per service.
     */
    public void publishBatch(List<LogPayload> logs) {
        for (LogPayload log : logs) {
            publish(log);
        }
    }

    public void publish(LogPayload payload) {
        CompletableFuture<SendResult<String, LogPayload>> future =
                kafkaTemplate.send(incomingLogsTopic, payload.getServiceName(), payload);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish log to Kafka — service: {}, traceId: {}, error: {}",
                        payload.getServiceName(), payload.getTraceId(), ex.getMessage());
            } else {
                log.debug("Published log — service: {}, traceId: {}, partition: {}, offset: {}",
                        payload.getServiceName(),
                        payload.getTraceId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
}