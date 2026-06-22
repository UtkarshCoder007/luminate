package com.luminate.alerting.dispatch;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class AlertDispatchService {

    private final RedisTemplate<String, Long> redisTemplate;
    private final WebClient.Builder webClientBuilder;

    @Value("${luminate.alert.webhook-url:}")
    private String webhookUrl;

    @Value("${luminate.alert.zscore-threshold:3.0}")
    private double zScoreThreshold;

    private static final String ALERT_ACTIVE_PREFIX  = "alert:active:";
    private static final String ALERT_RECOVERY_PREFIX = "alert:recovery:";
    private static final int    RECOVERY_CYCLES_NEEDED = 3;

    /**
     * Fires an alert for a service that has breached the Z-score threshold.
     * Stores alert state in Redis to prevent duplicate firing.
     * Skips if alert is already active for this service.
     */
    public void fireAlert(String serviceName, double zScore,
                          double currentRate, double mean, double stdDev) {
        String alertKey = ALERT_ACTIVE_PREFIX + serviceName;

        // Store alert in Redis with 24hr expiry as safety net
        Map<String, String> alertData = new HashMap<>();
        alertData.put("serviceName", serviceName);
        alertData.put("zScore", String.valueOf(zScore));
        alertData.put("currentRate", String.valueOf(currentRate));
        alertData.put("mean", String.valueOf(mean));
        alertData.put("stdDev", String.valueOf(stdDev));
        alertData.put("firedAt", Instant.now().toString());

        redisTemplate.opsForHash().putAll(alertKey, alertData);
        redisTemplate.expire(alertKey, Duration.ofHours(24));

        // Reset recovery counter
        redisTemplate.delete(ALERT_RECOVERY_PREFIX + serviceName);

        log.error("ALERT FIRED — service: {}, Z-score: {}, current: {}/min, baseline mean: {}/min",
                serviceName,
                String.format("%.2f", zScore),
                String.format("%.0f", currentRate),
                String.format("%.0f", mean));

        // Dispatch webhook if configured
        if (webhookUrl != null && !webhookUrl.isBlank()) {
            dispatchWebhook(serviceName, zScore, currentRate, mean);
        }
    }

    /**
     * Tracks recovery cycles. Clears alert after 3 consecutive healthy cycles.
     * Returns true if service has fully recovered.
     */
    public boolean trackRecovery(String serviceName) {
        String recoveryKey = ALERT_RECOVERY_PREFIX + serviceName;

        Long cycles = redisTemplate.opsForValue().increment(recoveryKey);

        if (cycles == null) return false;

        log.info("Recovery cycle {}/{} for service: {}",
                cycles, RECOVERY_CYCLES_NEEDED, serviceName);

        if (cycles >= RECOVERY_CYCLES_NEEDED) {
            // Service has recovered — clear alert state
            redisTemplate.delete(ALERT_ACTIVE_PREFIX + serviceName);
            redisTemplate.delete(recoveryKey);
            log.info("ALERT CLEARED — service: {} has recovered", serviceName);
            return true;
        }

        return false;
    }

    /**
     * Resets recovery progress if service spikes again during recovery.
     */
    public void resetRecovery(String serviceName) {
        redisTemplate.delete(ALERT_RECOVERY_PREFIX + serviceName);
    }

    /**
     * Returns all active alert data for the anomalies endpoint.
     */
    public java.util.List<Map<String, Object>> getActiveAlerts() {
        java.util.Set<String> keys = redisTemplate.keys(ALERT_ACTIVE_PREFIX + "*");
        if (keys == null || keys.isEmpty()) return java.util.List.of();

        return keys.stream()
                .map(key -> {
                    Map<Object, Object> data = redisTemplate.opsForHash().entries(key);
                    Map<String, Object> alert = new HashMap<>();
                    data.forEach((k, v) -> alert.put(k.toString(), v));
                    return alert;
                })
                .toList();
    }

    private void dispatchWebhook(String serviceName, double zScore,
                                 double currentRate, double mean) {
        try {
            Map<String, Object> payload = Map.of(
                    "alert", "ANOMALY_DETECTED",
                    "service", serviceName,
                    "zScore", zScore,
                    "currentRate", currentRate,
                    "baselineMean", mean,
                    "timestamp", Instant.now().toString(),
                    "message", String.format(
                            "Service %s is producing %.0f logs/min " +
                                    "(%.1f standard deviations above normal)",
                            serviceName, currentRate, zScore)
            );

            webClientBuilder.build()
                    .post()
                    .uri(webhookUrl)
                    .bodyValue(payload)
                    .retrieve()
                    .toBodilessEntity()
                    .subscribe(
                            response -> log.info("Webhook delivered for: {}", serviceName),
                            error -> log.error("Webhook delivery failed for: {}, error: {}",
                                    serviceName, error.getMessage())
                    );
        } catch (Exception e) {
            log.error("Webhook dispatch failed for service: {}", serviceName, e);
        }
    }
}