package com.luminate.search.controller;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.aggregations.CalendarInterval;
import co.elastic.clients.elasticsearch._types.aggregations.DateHistogramBucket;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.*;

@Slf4j
@RestController
@RequestMapping("/api/v1/metrics")
@RequiredArgsConstructor
public class MetricsController {

    private final ElasticsearchClient elasticsearchClient;

    private static final String INDEX_PATTERN = "luminate-logs-*";

    /**
     * Returns log volume bucketed by time interval grouped by service.
     * Used by the dashboard volume chart.
     *
     * GET /api/v1/metrics/volume?interval=1h&range=6h
     * interval: 5m, 15m, 1h
     * range: 1h, 6h, 24h
     */
    @GetMapping("/volume")
    public Mono<ResponseEntity<Map<String, Object>>> getVolume(
            @RequestParam(defaultValue = "15m") String interval,
            @RequestParam(defaultValue = "6h") String range) {

        return Mono.fromCallable(() -> {
            try {
                CalendarInterval calInterval = switch (interval) {
                    case "5m"  -> CalendarInterval.Minute;
                    case "1h"  -> CalendarInterval.Hour;
                    default    -> CalendarInterval.Minute;
                };

                var response = elasticsearchClient.search(s -> s
                                .index(INDEX_PATTERN)
                                .size(0)
                                .query(q -> q.range(r -> r
                                        .date(d -> d
                                                .field("timestamp")
                                                .gte("now-" + range)
                                        )
                                ))
                                .aggregations("by_service", a -> a
                                        .terms(t -> t
                                                .field("serviceName")
                                                .size(20)
                                        )
                                        .aggregations("over_time", inner -> inner
                                                .dateHistogram(dh -> dh
                                                        .field("timestamp")
                                                        .calendarInterval(calInterval)
                                                )
                                                .aggregations("by_level", level -> level
                                                        .terms(t -> t
                                                                .field("logLevel")
                                                                .size(5)
                                                        )
                                                )
                                        )
                                ),
                        Void.class
                );

                // Extract results into chart-friendly structure
                List<Map<String, Object>> series = new ArrayList<>();

                var serviceTerms = response.aggregations()
                        .get("by_service")
                        .sterms();

                for (StringTermsBucket serviceBucket : serviceTerms.buckets().array()) {
                    String serviceName = serviceBucket.key().stringValue();

                    List<Map<String, Object>> dataPoints = new ArrayList<>();

                    for (DateHistogramBucket timeBucket :
                            serviceBucket.aggregations()
                                    .get("over_time")
                                    .dateHistogram()
                                    .buckets().array()) {

                        Map<String, Object> point = new LinkedHashMap<>();
                        point.put("timestamp", timeBucket.keyAsString());
                        point.put("total", timeBucket.docCount());

                        // Break down by log level
                        Map<String, Long> levels = new LinkedHashMap<>();
                        for (var levelBucket : timeBucket.aggregations()
                                .get("by_level")
                                .sterms()
                                .buckets().array()) {
                            levels.put(
                                    levelBucket.key().stringValue(),
                                    levelBucket.docCount()
                            );
                        }
                        point.put("levels", levels);
                        dataPoints.add(point);
                    }

                    Map<String, Object> serviceData = new LinkedHashMap<>();
                    serviceData.put("service", serviceName);
                    serviceData.put("data", dataPoints);
                    series.add(serviceData);
                }

                return ResponseEntity.ok(Map.<String, Object>of(
                        "range", range,
                        "interval", interval,
                        "series", series
                ));

            } catch (Exception e) {
                log.error("Volume metrics query failed: {}", e.getMessage());
                throw new RuntimeException("Volume metrics failed", e);
            }
        });
    }
}