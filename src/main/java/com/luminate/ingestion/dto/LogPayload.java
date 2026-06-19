package com.luminate.ingestion.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LogPayload {

    @NotNull(message = "timestamp is required")
    private Instant timestamp;

    @NotBlank(message = "serviceName is required")
    private String serviceName;

    @NotBlank(message = "logLevel is required")
    @Pattern(
            regexp = "^(DEBUG|INFO|WARN|ERROR|FATAL)$",
            message = "logLevel must be one of: DEBUG, INFO, WARN, ERROR, FATAL"
    )
    private String logLevel;

    @NotBlank(message = "message is required")
    private String message;

    @NotBlank(message = "traceId is required")
    private String traceId;

    // Optional — only present for ERROR and FATAL logs
    private String stackTrace;
}