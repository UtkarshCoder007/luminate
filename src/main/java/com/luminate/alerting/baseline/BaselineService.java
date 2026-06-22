package com.luminate.alerting.baseline;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class BaselineService {

    private final RedisTemplate<String, Long> redisTemplate;

    private static final String BASELINE_PREFIX = "baseline:";
    private static final String ALERT_ACTIVE_PREFIX = "alert:active:";

    /**
     * Stores computed baseline for a service in a Redis Hash.
     * Key: baseline:payment-service
     * Fields: mean, stdDev, sampleSize, updatedAt
     */
    public void saveBaseline(String serviceName, double mean,
                             double stdDev, long sampleSize) {
        String key = BASELINE_PREFIX + serviceName;

        Map<String, String> baseline = new HashMap<>();
        baseline.put("mean", String.valueOf(mean));
        baseline.put("stdDev", String.valueOf(stdDev));
        baseline.put("sampleSize", String.valueOf(sampleSize));
        baseline.put("updatedAt", String.valueOf(Instant.now().toEpochMilli()));

        redisTemplate.opsForHash().putAll(key, baseline);

        log.info("Saved baseline for {} — mean: {:.2f}, stdDev: {:.2f}, samples: {}",
                serviceName, mean, stdDev, sampleSize);
    }

    /**
     * Retrieves the baseline for a service from Redis.
     * Returns null if no baseline exists yet — caller handles cold start.
     */
    public ServiceBaseline getBaseline(String serviceName) {
        String key = BASELINE_PREFIX + serviceName;
        Map<Object, Object> entries = redisTemplate.opsForHash().entries(key);

        if (entries == null || entries.isEmpty()) {
            return null;
        }

        try {
            double mean = Double.parseDouble(
                    entries.getOrDefault("mean", "0").toString());
            double stdDev = Double.parseDouble(
                    entries.getOrDefault("stdDev", "0").toString());
            long sampleSize = Long.parseLong(
                    entries.getOrDefault("sampleSize", "0").toString());
            long updatedAt = Long.parseLong(
                    entries.getOrDefault("updatedAt", "0").toString());

            return new ServiceBaseline(serviceName, mean, stdDev,
                    sampleSize, updatedAt);
        } catch (NumberFormatException e) {
            log.error("Corrupted baseline data for service: {}", serviceName);
            return null;
        }
    }

    /**
     * Returns all known service names by scanning baseline keys in Redis.
     */
    public java.util.Set<String> getKnownServices() {
        java.util.Set<String> keys = redisTemplate.keys(BASELINE_PREFIX + "*");
        if (keys == null) return java.util.Set.of();
        return keys.stream()
                .map(k -> k.replace(BASELINE_PREFIX, ""))
                .collect(java.util.stream.Collectors.toSet());
    }

    public boolean isAlertActive(String serviceName) {
        return Boolean.TRUE.equals(
                redisTemplate.hasKey(ALERT_ACTIVE_PREFIX + serviceName));
    }

    // ── Inner record ─────────────────────────────────────────────────────────

    public record ServiceBaseline(
            String serviceName,
            double mean,
            double stdDev,
            long sampleSize,
            long updatedAt
    ) {}
}