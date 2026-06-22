package com.luminate.alerting.controller;

import com.luminate.alerting.dispatch.AlertDispatchService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/metrics")
@RequiredArgsConstructor
public class AlertController {

    private final AlertDispatchService alertDispatchService;

    /**
     * Returns all currently active anomaly alerts.
     * Pulled directly from Redis — no Elasticsearch query needed.
     *
     * GET /api/v1/metrics/anomalies
     */
    @GetMapping("/anomalies")
    public Mono<ResponseEntity<Map<String, Object>>> getAnomalies() {
        return Mono.fromCallable(() -> {
            List<Map<String, Object>> alerts =
                    alertDispatchService.getActiveAlerts();

            return ResponseEntity.ok(Map.<String, Object>of(
                    "activeAlerts", alerts.size(),
                    "alerts", alerts
            ));
        });
    }
}