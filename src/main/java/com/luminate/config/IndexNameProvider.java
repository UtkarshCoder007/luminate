package com.luminate.config;

import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Component("indexNameProvider")
public class IndexNameProvider {

    private static final String PREFIX = "luminate-logs-";
    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public String getIndexName() {
        return PREFIX + LocalDate.now().format(FORMATTER);
    }

    public String getIndexNameForDate(LocalDate date) {
        return PREFIX + date.format(FORMATTER);
    }

    public String getWildcardPattern() {
        return PREFIX + "*";
    }
}