package com.luminate.alerting.engine;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.aggregations.*;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import com.luminate.alerting.baseline.BaselineService;
import com.luminate.alerting.dispatch.AlertDispatchService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;

@Slf4j
@Component
@RequiredArgsConstructor
public class AnomalyDetectionEngine {

    private final ElasticsearchClient elasticsearchClient;
    private final BaselineService baselineService;
    private final AlertDispatchService alertDispatchService;
    private final RedisTemplate<String, Long> redisTemplate;

    @Value("${luminate.alert.zscore-threshold:3.0}")
    private double zScoreThreshold;

    private static final String RATE_LIMIT_PREFIX = "rate_limit:";
    private static final String INDEX_PATTERN     = "luminate-logs-*";
    private static final long   MIN_SAMPLE_SIZE   = 100;
    private static final double RECOVERY_THRESHOLD = 1.5;

    /**
     * Runs every minute.
     * Reads current ingest rate from Redis rate limit counters.
     * Computes Z-score against stored baselines.
     * Fires or tracks alerts accordingly.
     */
    @Scheduled(fixedRate = 60000)
    public void evaluateAnomalies() {
        log.debug("Running Z-score evaluation");

        Set<String> services = baselineService.getKnownServices();
        if (services.isEmpty()) {
            log.debug("No baselines found — skipping evaluation");
            return;
        }

        for (String serviceName : services) {
            try {
                evaluateService(serviceName);
            } catch (Exception e) {
                log.error("Anomaly evaluation failed for service: {}", serviceName, e);
                // Fail open — never let one service failure stop evaluation of others
            }
        }
    }

    /**
     * Runs every hour.
     * Queries Elasticsearch using extended_stats aggregation.
     * Skips services with active alerts to prevent baseline poisoning.
     */
    @Scheduled(fixedRate = 3600000)
    public void refreshBaselines() {
        log.info("Starting hourly baseline refresh");

        Set<String> services = getServicesFromRateLimitKeys();

        for (String serviceName : services) {
            if (baselineService.isAlertActive(serviceName)) {
                log.info("Skipping baseline refresh for {} — alert is active", serviceName);
                continue;
            }
            try {
                computeAndStoreBaseline(serviceName);
            } catch (Exception e) {
                log.error("Baseline refresh failed for service: {}", serviceName, e);
            }
        }

        log.info("Baseline refresh complete for {} services", services.size());
    }

    private void evaluateService(String serviceName) {
        BaselineService.ServiceBaseline baseline =
                baselineService.getBaseline(serviceName);

        if (baseline == null) {
            log.debug("No baseline for {} yet — skipping", serviceName);
            return;
        }

        if (baseline.sampleSize() < MIN_SAMPLE_SIZE) {
            log.debug("Insufficient samples for {} ({}) — skipping",
                    serviceName, baseline.sampleSize());
            return;
        }

        // Read current rate from Redis rate limit counter (Option A)
        Double currentRate = getCurrentRate(serviceName);
        if (currentRate == null) {
            log.debug("No current rate data for {}", serviceName);
            return;
        }

        // Guard against zero stdDev — flat signal, can't compute Z-score
        if (baseline.stdDev() < 0.01) {
            log.debug("StdDev near zero for {} — skipping Z-score", serviceName);
            return;
        }

        double zScore = (currentRate - baseline.mean()) / baseline.stdDev();

        log.debug("Z-score for {} — Z: {:.2f}, rate: {:.0f}, mean: {:.0f}",
                serviceName, zScore, currentRate, baseline.mean());

        boolean alertActive = baselineService.isAlertActive(serviceName);

        if (zScore > zScoreThreshold) {
            if (!alertActive) {
                // New anomaly — fire alert
                alertDispatchService.fireAlert(
                        serviceName, zScore, currentRate,
                        baseline.mean(), baseline.stdDev());
            } else {
                // Ongoing anomaly — reset recovery, log only
                alertDispatchService.resetRecovery(serviceName);
                log.warn("Ongoing anomaly for {} — Z: {}",
                        serviceName,
                        String.format("%.2f", zScore));
            }
        } else if (zScore < RECOVERY_THRESHOLD && alertActive) {
            // Service trending toward recovery
            alertDispatchService.trackRecovery(serviceName);
        }
    }

    /**
     * Queries Elasticsearch using date histogram + extended_stats aggregation.
     * Never fetches raw documents — only aggregated math.
     */
    private void computeAndStoreBaseline(String serviceName) throws Exception {
        SearchResponse<Void> response = elasticsearchClient.search(s -> s
                        .index(INDEX_PATTERN)
                        .size(0) // fetch zero documents — aggregations only
                        .query(q -> q.bool(b -> b
                                .must(m -> m.term(t -> t
                                        .field("serviceName")
                                        .value(serviceName)
                                ))
                                .must(m -> m.range(r -> r
                                        .date(d -> d
                                                .field("timestamp")
                                                .gte("now-7d")
                                        )
                                ))
                        ))
                        .aggregations("per_minute", a -> a
                                .dateHistogram(dh -> dh
                                        .field("timestamp")
                                        .calendarInterval(CalendarInterval.Minute)
                                )
                                .aggregations("stats", inner -> inner
                                        .extendedStats(es -> es
                                                .field("_index") // count docs per bucket
                                                .sigma(2.0)
                                        )
                                )
                        ),
                Void.class
        );

        // Extract the per-minute bucket counts for statistical analysis
        DateHistogramAggregate histogram = response.aggregations()
                .get("per_minute")
                .dateHistogram();

        if (histogram.buckets().array().isEmpty()) {
            log.warn("No data found for service: {} in last 7 days", serviceName);
            return;
        }

        // Compute mean and stdDev from bucket doc counts
        long[] counts = histogram.buckets().array().stream()
                .mapToLong(DateHistogramBucket::docCount)
                .toArray();

        double mean = calculateMean(counts);
        double stdDev = calculateStdDev(counts, mean);
        long sampleSize = counts.length;

        baselineService.saveBaseline(serviceName, mean, stdDev, sampleSize);
    }

    private Double getCurrentRate(String serviceName) {
        String key = RATE_LIMIT_PREFIX + serviceName;
        Object value = redisTemplate.opsForValue().get(key);
        if (value == null) return null;
        try {
            return Double.parseDouble(value.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private Set<String> getServicesFromRateLimitKeys() {
        Set<String> keys = redisTemplate.keys(RATE_LIMIT_PREFIX + "*");
        if (keys == null) return Set.of();
        return keys.stream()
                .map(k -> k.replace(RATE_LIMIT_PREFIX, ""))
                .collect(java.util.stream.Collectors.toSet());
    }

    private double calculateMean(long[] values) {
        if (values.length == 0) return 0;
        long sum = 0;
        for (long v : values) sum += v;
        return (double) sum / values.length;
    }

    private double calculateStdDev(long[] values, double mean) {
        if (values.length < 2) return 0;
        double sumSquaredDiffs = 0;
        for (long v : values) {
            double diff = v - mean;
            sumSquaredDiffs += diff * diff;
        }
        return Math.sqrt(sumSquaredDiffs / (values.length - 1));
    }
}