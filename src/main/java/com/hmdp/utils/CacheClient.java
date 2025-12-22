package com.hmdp.utils;


import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.sql.Time;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Component
@Slf4j
public class CacheClient {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 存redis
     * @param key
     * @param value
     * @param time
     * @param timeUnit
     */
    public void set(String key, Object value, Long time, TimeUnit timeUnit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, timeUnit);
    }

    /**
     * 存缓存(逻辑缓存版)
     *
     * @param key
     * @param value
     * @param time
     * @param timeUnit
     */
    public void setWithLogicExpire(String key, Object value, Long time, TimeUnit timeUnit) {
        // 封装一下value
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(timeUnit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    /**
     * 缓存穿透版
     * @param id
     * @return
     */
    public <R, ID> R queryWithPassThrough(String keyPrefix, Function<ID, R> dbFallback, Long dataTTL, TimeUnit unit, ID id, Class<R> type) {
        String key = keyPrefix + id;
        //从redis查询商铺缓存
        String Json = stringRedisTemplate.opsForValue().get(key);
        // redis查到了=>直接返回
        if (!StrUtil.isBlank(Json)) {
            return JSONUtil.toBean(Json, type);
        }
        if (Json != null) {
            return null;
        }
        // redis没查到，查数据库
        R data = dbFallback.apply(id);
        if (data == null) {
            // 写空值进缓存
            set(key, "", CACHE_NULL_TTL, unit);
            return null;
        }
        // 把数据库查出来的写入redis
        set(key, data, dataTTL, unit);
        return data;
    }

    // 线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);


    /**
     * 查询(逻辑过期版)
     * @param id
     * @param keyPrefix
     * @param lockKeyPrefix
     * @param dbFallback
     * @param type
     * @param time
     * @param timeUnit
     * @return
     * @param <ID>
     * @param <R>
     */
    public <ID, R> R queryWithLogicExpire
    (ID id, String keyPrefix, String lockKeyPrefix, Function<ID, R> dbFallback, Class<R> type, Long time,  TimeUnit timeUnit) {
        String key = keyPrefix + id;
        String Json = stringRedisTemplate.opsForValue().get(key);
        //是否查询到
        if (StrUtil.isBlank(Json)) {
             return null;
        }
        // 查到了
        // 反序列化
        RedisData redisData = JSONUtil.toBean(Json, RedisData.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        R data = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        // 查看是否过期
        if (!expireTime.isBefore(LocalDateTime.now())) {
            //没过期
            return data;
        }
        // 过期了，获取锁，开一个线程缓存重建
        String lockKey = lockKeyPrefix + id;
        boolean islock = tryLock(lockKey);
        if (islock) {
            // 获取到锁了, 开始重建缓存
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    R r = dbFallback.apply(id);
                    setWithLogicExpire(key, r, time, timeUnit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unLock(lockKey);
                }
            });
        }
        return data;
    }

    /**
     * 获取锁
     * @param key
     * @return
     */
    public boolean tryLock(String key) {
        Boolean b = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        // 这里直接return b不行，b为Boolean包装类型，直接return会自动拆箱，执行return b.booleanValue();而b可能为true, false, null(空指针报错)
        return BooleanUtil.isTrue(b);
    }

    /**
     * 释放锁
     * @param key
     */
    public void unLock(String key) {
        stringRedisTemplate.delete(key);
    }

}
