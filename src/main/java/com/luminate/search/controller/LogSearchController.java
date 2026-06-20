package com.luminate.search.controller;

import com.luminate.model.LogDocument;
import com.luminate.search.repository.LogRepository;
import com.luminate.search.service.LogSearchService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class LogSearchController {

    private final LogSearchService logSearchService;

    /**
     * Universal search endpoint.
     * All params optional — returns 50 most recent logs if none provided.
     *
     * GET /api/v1/search?query=NullPointer&service=payment-service&level=ERROR&from=now-15m&to=now&pageSize=50&cursor=...
     */
    @GetMapping("/search")
    public Mono<ResponseEntity<Map<String, Object>>> search(
            @RequestParam(required = false) String query,
            @RequestParam(required = false) String service,
            @RequestParam(required = false) String level,
            @RequestParam(required = false) String from,
            @RequestParam(required = false) String to,
            @RequestParam(defaultValue = "50") int pageSize,
            @RequestParam(required = false) String[] cursor) {

        return Mono.fromCallable(() -> {
            LogRepository.SearchResult result = logSearchService.search(
                    query, service, level, from, to, pageSize, cursor
            );
            Map<String, Object> response = logSearchService.buildSearchResponse(result);
            return ResponseEntity.ok(response);
        });
    }

    /**
     * Distributed trace endpoint.
     * Returns the full chronological journey of a request across all services.
     *
     * GET /api/v1/logs/trace/abc-123-xyz
     */
    @GetMapping("/logs/trace/{traceId}")
    public Mono<ResponseEntity<Map<String, Object>>> getTrace(
            @PathVariable String traceId) {

        return Mono.fromCallable(() -> {
            List<LogDocument> trace = logSearchService.getTrace(traceId);

            if (trace.isEmpty()) {
                return ResponseEntity.ok(Map.<String, Object>of(
                        "traceId", traceId,
                        "found", 0,
                        "journey", List.of()
                ));
            }

            return ResponseEntity.ok(Map.<String, Object>of(
                    "traceId", traceId,
                    "found", trace.size(),
                    "services", trace.stream()
                            .map(LogDocument::getServiceName)
                            .distinct()
                            .toList(),
                    "journey", trace
            ));
        });
    }
}