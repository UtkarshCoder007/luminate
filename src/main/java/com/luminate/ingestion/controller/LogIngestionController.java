package com.luminate.ingestion.controller;

import com.luminate.ingestion.dto.LogPayload;
import com.luminate.ingestion.ratelimit.RateLimiterService;
import com.luminate.kafka.producer.LogEventProducer;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/v1/logs")
@RequiredArgsConstructor
public class LogIngestionController {

    private final RateLimiterService rateLimiterService;
    private final LogEventProducer logEventProducer;

    /**
     * Batch ingestion endpoint.
     * Validates schema, checks rate limit per service, publishes to Kafka.
     * Always returns 202 Accepted — never blocks the calling service.
     */
    @PostMapping("/batch")
    public Mono<ResponseEntity<Map<String, Object>>> ingestBatch(
            @Valid @RequestBody List<LogPayload> logs) {

        if (logs == null || logs.isEmpty()) {
            return Mono.just(ResponseEntity
                    .badRequest()
                    .body(Map.of(
                            "status", "rejected",
                            "reason", "Payload must contain at least one log entry"
                    )));
        }

        // Group by service and check rate limit per service
        Map<String, List<LogPayload>> groupedByService = logs.stream()
                .collect(java.util.stream.Collectors.groupingBy(LogPayload::getServiceName));

        for (String serviceName : groupedByService.keySet()) {
            if (!rateLimiterService.isAllowed(serviceName)) {
                return Mono.just(ResponseEntity
                        .status(HttpStatus.TOO_MANY_REQUESTS)
                        .body(Map.of(
                                "status", "rejected",
                                "reason", "Rate limit exceeded for service: " + serviceName
                        )));
            }
        }

        // Publish all logs to Kafka asynchronously
        logEventProducer.publishBatch(logs);

        log.info("Accepted batch of {} logs from {} service(s)",
                logs.size(), groupedByService.size());

        return Mono.just(ResponseEntity
                .accepted()
                .body(Map.of(
                        "status", "accepted",
                        "accepted", logs.size()
                )));
    }
}