package com.luminate.search.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HighlightField;
import com.luminate.model.LogDocument;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Repository
@RequiredArgsConstructor
public class LogRepository {

    private final ElasticsearchClient elasticsearchClient;

    private static final String INDEX_PATTERN = "luminate-logs-*";
    private static final int MAX_PAGE_SIZE = 500;
    private static final int DEFAULT_PAGE_SIZE = 50;

    /**
     * Universal search — combines must filters with optional fuzzy text search.
     * Uses search_after for cursor-based pagination.
     */
    public SearchResult search(SearchParams params) {
        try {
            int size = Math.min(
                    params.getPageSize() > 0 ? params.getPageSize() : DEFAULT_PAGE_SIZE,
                    MAX_PAGE_SIZE
            );

            // ── Build Bool Query ──────────────────────────────────────────────
            BoolQuery.Builder boolQuery = new BoolQuery.Builder();
            boolean hasFilters = false;

            // must: serviceName filter
            if (params.getServiceName() != null && !params.getServiceName().isBlank()) {
                boolQuery.must(m -> m.term(t -> t
                        .field("serviceName")
                        .value(params.getServiceName())
                ));
                hasFilters = true;
            }

            // must: logLevel filter
            if (params.getLogLevel() != null && !params.getLogLevel().isBlank()) {
                boolQuery.must(m -> m.term(t -> t
                        .field("logLevel")
                        .value(params.getLogLevel())
                ));
                hasFilters = true;
            }

            // must: timestamp range filter
            if (params.getFrom() != null || params.getTo() != null) {
                boolQuery.must(m -> m.range(r -> {
                    var range = r.date(d -> {
                        var builder = d.field("timestamp");
                        if (params.getFrom() != null) {
                            builder = builder.gte(params.getFrom());
                        }
                        if (params.getTo() != null) {
                            builder = builder.lte(params.getTo());
                        }
                        return builder;
                    });
                    return range;
                }));
                hasFilters = true;
            }

            // should: fuzzy text search on message and stackTrace
            if (params.getQuery() != null && !params.getQuery().isBlank()) {
                boolQuery.should(s -> s.fuzzy(f -> f
                        .field("message")
                        .value(params.getQuery())
                        .fuzziness("AUTO")
                        .maxExpansions(50)
                ));
                boolQuery.should(s -> s.fuzzy(f -> f
                        .field("stackTrace")
                        .value(params.getQuery())
                        .fuzziness("AUTO")
                        .maxExpansions(50)
                ));
                hasFilters = true;
            }

            // Default: matchAll if no params provided
            Query finalQuery = hasFilters
                    ? Query.of(q -> q.bool(boolQuery.build()))
                    : Query.of(q -> q.matchAll(m -> m));

            // ── Build Search Request ──────────────────────────────────────────
            SearchRequest.Builder requestBuilder = new SearchRequest.Builder()
                    .index(INDEX_PATTERN)
                    .query(finalQuery)
                    .size(size)
                    .sort(s -> s.field(f -> f
                            .field("timestamp")
                            .order(SortOrder.Desc)
                    ))
                    .sort(s -> s.score(sc -> sc))
                    // Highlighting on message and stackTrace
                    .highlight(h -> h
                            .fields("message", HighlightField.of(hf -> hf))
                            .fields("stackTrace", HighlightField.of(hf -> hf))
                            .preTags("<em>")
                            .postTags("</em>")
                    );

            // Apply cursor if provided
            if (params.getCursor() != null && params.getCursor().length == 2) {
                requestBuilder.searchAfter(
                        FieldValue.of(params.getCursor()[0]),
                        FieldValue.of(params.getCursor()[1])
                );
            }

            SearchResponse<LogDocument> response = elasticsearchClient
                    .search(requestBuilder.build(), LogDocument.class);

            // ── Extract Results ───────────────────────────────────────────────
            List<LogSearchHit> hits = new ArrayList<>();
            List<Hit<LogDocument>> rawHits = response.hits().hits();

            for (Hit<LogDocument> hit : rawHits) {
                LogDocument doc = hit.source();
                Map<String, List<String>> highlights = hit.highlight();
                hits.add(new LogSearchHit(doc, highlights));
            }

            // Build next cursor from last hit's sort values
            String[] nextCursor = null;
            if (!rawHits.isEmpty()) {
                Hit<LogDocument> lastHit = rawHits.get(rawHits.size() - 1);
                List<FieldValue> sortValues = lastHit.sort();
                if (sortValues != null && !sortValues.isEmpty()) {
                    nextCursor = new String[]{
                            sortValues.get(0).toString()
                    };
                }
            }

            long total = response.hits().total() != null
                    ? response.hits().total().value()
                    : 0;

            return new SearchResult(hits, total, size, nextCursor);

        } catch (IOException e) {
            log.error("Elasticsearch search failed: {}", e.getMessage());
            throw new RuntimeException("Search failed", e);
        }
    }

    /**
     * Distributed tracing — exact traceId match, sorted chronologically.
     * Searches across all indices, returns full journey in one shot.
     */
    public List<LogDocument> findByTraceId(String traceId) {
        try {
            SearchResponse<LogDocument> response = elasticsearchClient.search(s -> s
                            .index(INDEX_PATTERN)
                            .query(q -> q.term(t -> t
                                    .field("traceId")
                                    .value(traceId)
                            ))
                            .sort(sort -> sort.field(f -> f
                                    .field("timestamp")
                                    .order(SortOrder.Asc)
                            ))
                            .sort(sort -> sort.score(sc -> sc))
                            .size(1000), // hard cap — single trace won't realistically exceed this
                    LogDocument.class
            );

            return response.hits().hits().stream()
                    .map(Hit::source)
                    .toList();

        } catch (IOException e) {
            log.error("Trace lookup failed for traceId: {}, error: {}", traceId, e.getMessage());
            throw new RuntimeException("Trace lookup failed", e);
        }
    }

    // ── Inner classes for structured results ─────────────────────────────────

    public record SearchParams(
            String query,
            String serviceName,
            String logLevel,
            String from,
            String to,
            int pageSize,
            String[] cursor
    ) {
        public String getQuery() { return query; }
        public String getServiceName() { return serviceName; }
        public String getLogLevel() { return logLevel; }
        public String getFrom() { return from; }
        public String getTo() { return to; }
        public int getPageSize() { return pageSize; }
        public String[] getCursor() { return cursor; }
    }

    public record LogSearchHit(
            LogDocument document,
            Map<String, List<String>> highlights
    ) {}

    public record SearchResult(
            List<LogSearchHit> hits,
            long total,
            int pageSize,
            String[] nextCursor
    ) {}
}