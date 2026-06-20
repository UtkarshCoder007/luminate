package com.luminate.kafka.consumer;

import com.luminate.ingestion.dto.LogPayload;
import com.luminate.model.LogDocument;
import com.luminate.config.IndexNameProvider;
import com.luminate.kafka.dlt.DeadLetterTopicHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.IndexOperations;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class LogEventConsumer {

    private final ElasticsearchOperations elasticsearchOperations;
    private final IndexNameProvider indexNameProvider;
    private final DeadLetterTopicHandler deadLetterTopicHandler;

    private static final int MAX_RETRIES = 3;

    @KafkaListener(
            topics = "${luminate.kafka.topics.incoming-logs}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(List<LogPayload> logs,
                        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {

        log.info("Received batch of {} logs from topic: {}", logs.size(), topic);

        String indexName = indexNameProvider.getIndexName();
        ensureIndexExists(indexName);

        List<LogDocument> documents = logs.stream()
                .map(this::toDocument)
                .toList();

        indexWithRetry(documents, indexName, logs);
    }

    /**
     * Manual retry with exponential backoff.
     * Retries 3 times: 1s, 2s, 4s delays.
     * Routes to DLT after all retries are exhausted.
     */
    private void indexWithRetry(List<LogDocument> documents,
                                String indexName,
                                List<LogPayload> originalPayloads) {
        int attempt = 0;
        long delay = 1000;

        while (attempt < MAX_RETRIES) {
            try {
                elasticsearchOperations.save(documents, IndexCoordinates.of(indexName));
                log.info("Indexed {} documents into: {}", documents.size(), indexName);
                return;
            } catch (Exception e) {
                attempt++;
                log.warn("Elasticsearch indexing failed — attempt {}/{}: {}",
                        attempt, MAX_RETRIES, e.getMessage());

                if (attempt < MAX_RETRIES) {
                    try {
                        Thread.sleep(delay);
                        delay *= 2; // exponential backoff
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                } else {
                    log.error("All {} retries exhausted — routing {} logs to DLT",
                            MAX_RETRIES, originalPayloads.size());
                    originalPayloads.forEach(deadLetterTopicHandler::sendToDlt);
                }
            }
        }
    }

    private void ensureIndexExists(String indexName) {
        IndexCoordinates coordinates = IndexCoordinates.of(indexName);
        IndexOperations indexOps = elasticsearchOperations.indexOps(coordinates);
        if (!indexOps.exists()) {
            indexOps.create();
            log.info("Created new index: {}", indexName);
        }
    }

    private LogDocument toDocument(LogPayload payload) {
        return LogDocument.builder()
                .id(UUID.randomUUID().toString())
                .timestamp(payload.getTimestamp())
                .serviceName(payload.getServiceName())
                .logLevel(payload.getLogLevel())
                .message(payload.getMessage())
                .traceId(payload.getTraceId())
                .stackTrace(payload.getStackTrace())
                .build();
    }
}