package com.luminate.ingestion.ratelimit;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Slf4j
@Service
@RequiredArgsConstructor
public class RateLimiterService {

    private final RedisTemplate<String, Long> redisTemplate;

    @Value("${luminate.rate-limit.requests-per-minute}")
    private int requestsPerMinute;

    private static final String RATE_LIMIT_PREFIX = "rate_limit:";

    /**
     * Token Bucket algorithm via Redis.
     * Each serviceName gets its own counter, reset every 60 seconds.
     * Returns true if the request is allowed, false if rate limit exceeded.
     */
    public boolean isAllowed(String serviceName) {
        String key = RATE_LIMIT_PREFIX + serviceName;

        try {
            Long currentCount = redisTemplate.opsForValue().increment(key);

            if (currentCount == null) {
                log.warn("Redis returned null for key: {}", key);
                return true; // fail open — don't block if Redis has issues
            }

            // First request for this window — set expiry of 60 seconds
            if (currentCount == 1) {
                redisTemplate.expire(key, Duration.ofSeconds(60));
            }

            if (currentCount > requestsPerMinute) {
                log.warn("Rate limit exceeded for service: {} — count: {}/{}",
                        serviceName, currentCount, requestsPerMinute);
                return false;
            }

            return true;

        } catch (Exception e) {
            log.error("Redis error during rate limit check for service: {}", serviceName, e);
            return true; // fail open — never block ingestion due to Redis issues
        }
    }
}