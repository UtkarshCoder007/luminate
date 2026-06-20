package com.luminate.search.service;

import com.luminate.model.LogDocument;
import com.luminate.search.repository.LogRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class LogSearchService {

    private final LogRepository logRepository;

    /**
     * Universal search — delegates to repository with validated params.
     */
    public LogRepository.SearchResult search(
            String query,
            String serviceName,
            String logLevel,
            String from,
            String to,
            int pageSize,
            String[] cursor) {

        log.info("Search request — query: '{}', service: {}, level: {}, from: {}, to: {}",
                query, serviceName, logLevel, from, to);

        LogRepository.SearchParams params = new LogRepository.SearchParams(
                query, serviceName, logLevel, from, to, pageSize, cursor
        );

        return logRepository.search(params);
    }

    /**
     * Distributed trace lookup — returns complete request journey.
     */
    public List<LogDocument> getTrace(String traceId) {
        log.info("Trace lookup — traceId: {}", traceId);

        List<LogDocument> trace = logRepository.findByTraceId(traceId);

        log.info("Found {} log entries for traceId: {}", trace.size(), traceId);
        return trace;
    }

    /**
     * Maps SearchResult to a clean API response structure.
     */
    public Map<String, Object> buildSearchResponse(LogRepository.SearchResult result) {
        List<Map<String, Object>> hits = result.hits().stream()
                .map(hit -> {
                    Map<String, Object> entry = new java.util.LinkedHashMap<>();
                    LogDocument doc = hit.document();
                    if (doc != null) {
                        entry.put("id", doc.getId());
                        entry.put("timestamp", doc.getTimestamp());
                        entry.put("serviceName", doc.getServiceName());
                        entry.put("logLevel", doc.getLogLevel());
                        entry.put("message", doc.getMessage());
                        entry.put("traceId", doc.getTraceId());
                        entry.put("stackTrace", doc.getStackTrace());
                    }
                    if (hit.highlights() != null && !hit.highlights().isEmpty()) {
                        entry.put("highlights", hit.highlights());
                    }
                    return entry;
                })
                .toList();

        Map<String, Object> response = new java.util.LinkedHashMap<>();
        response.put("results", hits);
        response.put("total", result.total());
        response.put("pageSize", result.pageSize());
        response.put("nextCursor", result.nextCursor());
        return response;
    }
}