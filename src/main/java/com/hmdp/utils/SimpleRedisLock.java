package com.hmdp.utils;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock{

    private static final String KEY_PREFIX = "lock:";
    private static final String PREFIX_KEY = UUID.randomUUID().toString() + "-";
    private String name;
    private StringRedisTemplate stringRedisTemplate;
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    public SimpleRedisLock(String name, StringRedisTemplate stringRedisTemplate) {
        this.name = name;
        this.stringRedisTemplate = stringRedisTemplate;
    }
    @Override
    public boolean tryLock(long timeoutSec) {
        // 获取线程id作为key
        long threadId = Thread.currentThread().getId();
        Boolean success = stringRedisTemplate.opsForValue().setIfAbsent(KEY_PREFIX + name, PREFIX_KEY + threadId, timeoutSec, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(success); // 拆箱，防止null的情况
    }

    @Override
    public void unLock() {
        long threadId = Thread.currentThread().getId();
        stringRedisTemplate.execute(UNLOCK_SCRIPT,
                Collections.singletonList(KEY_PREFIX + name),
                PREFIX_KEY + threadId);
    }
    /*public void unLock() {
        String value = stringRedisTemplate.opsForValue().get(KEY_PREFIX + name);
        if (Objects.equals(value, PREFIX_KEY + Thread.currentThread().getId())) stringRedisTemplate.delete(KEY_PREFIX + name);
    }*/
}
