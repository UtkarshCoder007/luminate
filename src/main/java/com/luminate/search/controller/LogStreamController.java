package com.luminate.search.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.luminate.model.LogDocument;
import com.luminate.search.event.LogsIndexedEvent;
import com.luminate.search.service.LogSearchService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/v1/logs")
@RequiredArgsConstructor
public class LogStreamController {

    private final LogSearchService logSearchService;
    private final ObjectMapper objectMapper;

    /**
     * Multicast sink — one publisher, many subscribers.
     * Every connected SSE client receives every new log batch.
     * LATEST backpressure strategy — slow clients drop old events
     * rather than blocking the producer.
     */
    private final Sinks.Many<List<LogDocument>> logSink =
            Sinks.many().multicast().onBackpressureBuffer(256, false);

    /**
     * SSE endpoint for live log tailing.
     * Sends recent history on connect, then streams new logs as they arrive.
     *
     * GET /api/v1/logs/stream
     */
    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<String>> streamLogs() {
        // Fetch last 50 logs as initial history
        List<Map<String, Object>> history = getRecentHistory();

        // Initial event with history
        Flux<ServerSentEvent<String>> historyFlux = Flux.fromIterable(history)
                .map(doc -> {
                    try {
                        return ServerSentEvent.<String>builder()
                                .event("log")
                                .data(objectMapper.writeValueAsString(doc))
                                .build();
                    } catch (Exception e) {
                        return ServerSentEvent.<String>builder()
                                .event("log")
                                .data("{}")
                                .build();
                    }
                });

        // Keepalive ping every 30 seconds to prevent connection timeout
        Flux<ServerSentEvent<String>> keepalive = Flux.interval(Duration.ofSeconds(30))
                .map(tick -> ServerSentEvent.<String>builder()
                        .event("ping")
                        .data("keep-alive")
                        .build());

        // Live stream from sink
        Flux<ServerSentEvent<String>> liveFlux = logSink.asFlux()
                .flatMap(Flux::fromIterable)
                .map(doc -> {
                    try {
                        return ServerSentEvent.<String>builder()
                                .event("log")
                                .data(objectMapper.writeValueAsString(doc))
                                .build();
                    } catch (Exception e) {
                        log.error("Failed to serialize log document for SSE");
                        return ServerSentEvent.<String>builder()
                                .event("log")
                                .data("{}")
                                .build();
                    }
                });

        // Combine: history first, then live stream merged with keepalive
        return historyFlux.concatWith(Flux.merge(liveFlux, keepalive));
    }

    /**
     * Receives LogsIndexedEvent from the Kafka consumer.
     * Pushes new documents into the sink for all connected SSE clients.
     */
    @EventListener
    public void onLogsIndexed(LogsIndexedEvent event) {
        List<LogDocument> docs = event.getDocuments();
        log.debug("Pushing {} new logs to SSE stream", docs.size());
        logSink.tryEmitNext(docs);
    }

    private List<Map<String, Object>> getRecentHistory() {
        try {
            var result = logSearchService.search(
                    null, null, null, null, null, 50, null);
            return logSearchService.buildSearchResponse(result)
                    .entrySet().stream()
                    .filter(e -> e.getKey().equals("results"))
                    .map(e -> (List<Map<String, Object>>) e.getValue())
                    .findFirst()
                    .orElse(List.of());
        } catch (Exception e) {
            log.error("Failed to fetch SSE history: {}", e.getMessage());
            return List.of();
        }
    }
}