package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.IService;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private CacheClient cacheClient;

    /**
     * 根据id查询商铺信息
     * @param id 商铺id
     * @return 商铺详情数据
     */
    @Override
    public Result queryById(Long id) {
        // 缓存穿透
        // queryWithPassThrough(id);
        // cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, (x) -> getById(x), CACHE_SHOP_TTL, TimeUnit.MINUTES, id, Shop.class);
        // 用逻辑缓存来解决缓存击穿
        // Shop shop = queryWithLogicExpire(id);
        Shop shop = cacheClient.queryWithLogicExpire(id, CACHE_SHOP_KEY, LOCK_SHOP_KEY, this::getById, Shop.class, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        // 用互斥锁解决缓存击穿
        //Shop shop = queryWithMutex(id);
        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }
/*
    // 线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    public Shop queryWithLogicExpire(Long id) {
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //是否查询到
        if (StrUtil.isBlank(shopJson)) {
            return null;
        }
        // 查到了
        // 反序列化
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        // 查看是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            //没过期
            return shop;
        }
        // 过期了，获取锁，开一个线程缓存重建
        String lockKey = LOCK_SHOP_KEY + id;
        boolean islock = tryLock(lockKey);
        if (islock) {
            // 获取到锁了, 开始重建缓存
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    saveShop2Redis(id, 20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unLock(lockKey);
                }
            });
        }
        return shop;
    }

    *//**
     * 用互斥锁解决缓存击穿版
     * @param id
     * @return
     *//*
    public Shop queryWithMutex(Long id) {
        String key = CACHE_SHOP_KEY + id;
        //从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // redis查到了=>直接返回
        if (!StrUtil.isBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        // 查到了""空值
        if (shopJson != null) {
            return null;
        }
        // 先获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            // 没获取到锁
            if (!isLock) {
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            // 获取到锁了，先看看redis数据重建过没
            // redis没查到，查数据库
            shop = getById(id);
            // 模拟重建时间长的情况
            Thread.sleep(200);
            if (shop == null) {
                // 写控制进缓存
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            // 把数据库查出来的写入redis
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            unLock(lockKey);
        }
        return shop;
    }

    *//**
     * 缓存穿透版
     * @param id
     * @return
     *//*
    public Shop queryWithPassThrough(Long id) {
        String key = CACHE_SHOP_KEY + id;
        //从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // redis查到了=>直接返回
        if (!StrUtil.isBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        if (shopJson != null) {
            return null;
        }
        // redis没查到，查数据库
        Shop shop = this.getById(id);
        if (shop == null) {
            // 写控制进缓存
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 把数据库查出来的写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;
    }

    *//**
     * 获取锁
     * @param key
     * @return
     *//*
    private boolean tryLock(String key) {
        Boolean b = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        // 这里直接return b不行，b为Boolean包装类型，直接return会自动拆箱，执行return b.booleanValue();而b可能为true, false, null(空指针报错)
        return BooleanUtil.isTrue(b);
    }

    *//**
     * 释放锁
     * @param key
     *//*
    private void unLock(String key) {
        stringRedisTemplate.delete(key);
    }*/

    /**
     * 更新店铺信息
     * @param shop
     * @return
     */
    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        // 1.更新数据库
        updateById(shop);
        // 2.删缓存
        String key = CACHE_SHOP_KEY + id;
        stringRedisTemplate.delete(key);
        return Result.ok();
    }

    /**
     * 店铺查询
     * @param typeId
     * @param current
     * @param x
     * @param y
     * @return
     */

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {

        // 1. 如果没有传坐标，走普通分页查询
        if (x == null || y == null) {
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            return Result.ok(page.getRecords());
        }

        // 2. 有坐标，走 GEO 查询
        int pageSize = SystemConstants.DEFAULT_PAGE_SIZE;
        int from = (current - 1) * pageSize;
        int end = current * pageSize;

        // 3. Redis GEO 查询
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results =
                stringRedisTemplate.opsForGeo()
                        .search(
                                key,
                                GeoReference.fromCoordinate(x, y),
                                new Distance(5000), // 5km 范围
                                RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs()
                                        .includeDistance()
                                        .limit(end)
                        );

        // 4. 判空
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }

        // 5. 解析 shopId 和 distance
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> content =
                results.getContent();

        if (content.size() <= from) {
            return Result.ok(Collections.emptyList());
        }

        List<Long> shopIds = new ArrayList<>(content.size());
        Map<Long, Distance> distanceMap = new HashMap<>();

        content.stream()
                .skip(from)
                .forEach(result -> {
                    String shopIdStr = result.getContent().getName();
                    Long shopId = Long.valueOf(shopIdStr);
                    shopIds.add(shopId);
                    distanceMap.put(shopId, result.getDistance());
                });

        // 6. 根据 shopId 查询数据库（保持顺序）
        String idStr = StrUtil.join(",", shopIds);
        List<Shop> shops = query()
                .in("id", shopIds)
                .last("ORDER BY FIELD(id," + idStr + ")")
                .list();

        // 7. 封装距离信息
        for (Shop shop : shops) {
            Distance distance = distanceMap.get(shop.getId());
            if (distance != null) {
                shop.setDistance(distance.getValue());
            }
        }

        return Result.ok(shops);
    }


    /**
     * 存redis
     * @param id
     * @param expireSecond
     */
    public void saveShop2Redis(Long id, Long expireSecond) {
        // 查数据库
        Shop shop = getById(id);
        // 封装数据
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSecond));
        // 写入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }
}
