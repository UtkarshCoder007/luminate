package com.luminate.lifecycle;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.GetIndexResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Slf4j
@Component
@RequiredArgsConstructor
public class IndexLifecycleJob {

    private final ElasticsearchClient elasticsearchClient;

    @Value("${luminate.lifecycle.retention-days:15}")
    private int retentionDays;

    private static final String INDEX_PREFIX = "luminate-logs-";
    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /**
     * Runs daily at 2am.
     * Deletes the Elasticsearch index that is exactly retentionDays old.
     * Uses per-day index naming so deletion is O(1) — no document scanning.
     *
     * Why we delete by exact index name and not wildcard:
     * Deleting one specific index is atomic and instant.
     * Wildcard deletes are disabled in Elasticsearch by default for safety.
     */
    @Scheduled(cron = "0 0 2 * * *")
    public void deleteExpiredIndices() {
        LocalDate cutoffDate = LocalDate.now().minusDays(retentionDays);
        String indexName = INDEX_PREFIX + cutoffDate.format(FORMATTER);

        log.info("Lifecycle job running — checking index: {}", indexName);

        try {
            boolean exists = elasticsearchClient.indices()
                    .exists(e -> e.index(indexName))
                    .value();

            if (!exists) {
                log.debug("Index {} does not exist — nothing to delete", indexName);
                return;
            }

            // Get document count before deletion for audit log
            long docCount = getDocumentCount(indexName);

            // Delete the index
            elasticsearchClient.indices().delete(d -> d.index(indexName));

            log.info("Lifecycle job — deleted index: {}, documents removed: {}, " +
                    "retention window: {} days", indexName, docCount, retentionDays);

        } catch (Exception e) {
            log.error("Lifecycle job failed for index: {}, error: {}",
                    indexName, e.getMessage());
            // Fail gracefully — job will retry tomorrow night
        }
    }

    /**
     * For testing purposes — runs immediately on demand.
     * Not scheduled — called directly in tests.
     */
    public void deleteIndexForDate(LocalDate date) {
        String indexName = INDEX_PREFIX + date.format(FORMATTER);
        log.info("Manual deletion requested for index: {}", indexName);

        try {
            boolean exists = elasticsearchClient.indices()
                    .exists(e -> e.index(indexName))
                    .value();

            if (!exists) {
                log.info("Index {} does not exist — skipping", indexName);
                return;
            }

            long docCount = getDocumentCount(indexName);
            elasticsearchClient.indices().delete(d -> d.index(indexName));
            log.info("Manually deleted index: {}, documents removed: {}",
                    indexName, docCount);

        } catch (Exception e) {
            log.error("Manual deletion failed for index: {}", indexName, e);
        }
    }

    private long getDocumentCount(String indexName) {
        try {
            return elasticsearchClient.count(c -> c.index(indexName)).count();
        } catch (Exception e) {
            log.warn("Could not get document count for index: {}", indexName);
            return -1;
        }
    }
}