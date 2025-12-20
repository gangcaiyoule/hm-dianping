package com.hmdp.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
@Slf4j
public class RedisIdWorker {

    public static final long BEGIN_TIMESTAMP = LocalDateTime.
            of(2025, 1, 1, 0, 0, 0).
            toEpochSecond(ZoneOffset.UTC);
    public static int COUNT_BIT = 32;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    public long nextId(String prefix) {
        // 时间戳部分
        long timeStamp = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
        long stamp = BEGIN_TIMESTAMP - timeStamp;
        // 序列号部分

        String date = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        String key = "icr:" + prefix + date;
        long count = stringRedisTemplate.opsForValue().increment(key);
        return (stamp << COUNT_BIT) | count;
    }
}
