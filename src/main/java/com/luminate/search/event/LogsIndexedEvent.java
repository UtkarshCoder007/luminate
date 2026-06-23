package com.luminate.search.event;

import com.luminate.model.LogDocument;
import org.springframework.context.ApplicationEvent;

import java.util.List;

/**
 * Published by LogEventConsumer after successful Elasticsearch indexing.
 * Consumed by LogStreamController to push new logs to SSE clients.
 * This decouples the ingestion pipeline from the streaming layer.
 */
public class LogsIndexedEvent extends ApplicationEvent {

    private final List<LogDocument> documents;

    public LogsIndexedEvent(Object source, List<LogDocument> documents) {
        super(source);
        this.documents = documents;
    }

    public List<LogDocument> getDocuments() {
        return documents;
    }
}