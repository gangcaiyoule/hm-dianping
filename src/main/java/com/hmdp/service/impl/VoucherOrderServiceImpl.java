package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.conditions.query.QueryChainWrapper;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IVoucherService;
import com.hmdp.utils.*;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    private static final String QUEUENAME = "stream:orders";
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private CacheClient cacheClient;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource(name = "reddisonClient")
    private RedissonClient redissonClient;
    @Resource(name = "reddisonClient2")
    private RedissonClient redissonClient2;
    @Resource(name = "reddisonClient3")
    private RedissonClient redissonClient3;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }
    // private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024); // 阻塞队列
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor(); // 只有一个线程的线程池
    IVoucherOrderService proxy;
    @PostConstruct
    private void init() {
        try {
            stringRedisTemplate.opsForStream().createGroup(
                    QUEUENAME,
                    ReadOffset.from("0"),
                    "g1"
            );
        } catch (Exception e) {
            // 如果已经存在，会抛异常，忽略即可
            log.info("消费组已存在");
        }
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            while (true) {
                // 消息队列
                try {
                    // 从消息队列读取消息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(QUEUENAME, ReadOffset.lastConsumed())
                    );
                    // 判断是否获取成功
                    if (list == null || list.isEmpty()) continue;
                    // 获取成功，解析订单消息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 下单
                    handleVoucherOrder(voucherOrder);
                    // ACK确认
                    stringRedisTemplate.opsForStream().acknowledge(QUEUENAME, "g1", record.getId());
                } catch (Exception e) {
                    handlPendingList();
                    log.error("处理订单异常", e);
                }
            }
        }

        private void handlPendingList() {
            while (true) {
                // 消息队列
                try {
                    // 从消息队列读取消息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(QUEUENAME, ReadOffset.from("0"))
                    );
                    // 判断是否获取成功
                    if (list == null || list.isEmpty()) break;
                    // 获取成功，解析订单消息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 下单
                    handleVoucherOrder(voucherOrder);
                    // ACK确认
                    stringRedisTemplate.opsForStream().acknowledge(QUEUENAME, "g1", record.getId());
                } catch (Exception e) {
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                    log.error("处理pending-list", e);
                }
            }
        }
        /*public void run() {
            while (true) {
                // 阻塞队列
                try {
                    VoucherOrder voucherOrder = orderTasks.take();
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                }
            }
        }*/

        /**
         * 数据库操作
         * @param voucherOrder
         */
        private void handleVoucherOrder(VoucherOrder voucherOrder) {
            Long userId = voucherOrder.getUserId();
            // 联锁
            RLock lock1 = redissonClient.getLock("order:" + userId);
            RLock lock2 = redissonClient2.getLock("order:" + userId);
            RLock lock3 = redissonClient3.getLock("order:" + userId);

            RLock lock = redissonClient.getMultiLock(lock1, lock2, lock3);

            boolean getLock = lock.tryLock(); // 无参标识没抢到锁就返回null
            if (!getLock) log.error("一人只能下一单");
            // 获取到锁
            try {
                proxy.createVoucherOrder(voucherOrder);
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                lock.unlock();
            }
        }
    }
    /**
     * 购买秒杀卷
     * @param voucherId
     * @return
     */
    @Override
    public Result seckkillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order:");
        long result = stringRedisTemplate.execute(SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId)
        );
        if (result == 1) return Result.fail("库存不足");
        if (result == 2) return Result.fail("您已下过单了，请勿重复购买");

        // 数据库操作
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        return Result.ok(orderId);
    }
    /*public Result seckkillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long result = stringRedisTemplate.execute(SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId);
        if (result == 1) return Result.fail("库存不足");
        if (result == 2) return Result.fail("您已下过单了，请勿重复购买");
        // 加入到阻塞队列
        // 封装数据
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setVoucherId(voucherId);
        voucherOrder.setUserId(userId);
        long orderId = redisIdWorker.nextId("order:");
        voucherOrder.setId(orderId);
        // 数据库操作
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        orderTasks.add(voucherOrder);

        return Result.ok(orderId);
    }*/
    /**
     * 创建订单
     * @param voucherOrder
     * @return
     */
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if (count > 0) {
            log.error("用户已经购买过一次！");
            return;
        }
        // 减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId).gt("stock", 0)
                .update();
        if (!success) {
            log.error("库存不足");
            return;
        }
        // 存数据
        save(voucherOrder);
    }
    /*public Result seckkillVoucher(Long voucherId) {
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        if (voucher == null) {
            return Result.fail("未查询到此优惠卷");
        }
        // 判断是否开始/是否结束
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("抢购未开始");
        }
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("抢购已结束");
        }
        // 判断库存是否充足
        if (voucher.getStock() <= 0) {
            return Result.fail("库存不足");
        }
        // 检查该用户是否下过单，一人一单
        Long userId = UserHolder.getUser().getId();
        // SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        // redisson版锁
        // RLock lock = redissonClient.getLock("order:" + userId);
        // 联锁
        RLock lock1 = redissonClient.getLock("order:" + userId);
        RLock lock2 = redissonClient2.getLock("order:" + userId);
        RLock lock3 = redissonClient3.getLock("order:" + userId);

        RLock lock = redissonClient.getMultiLock(lock1, lock2, lock3);

        boolean getLock = lock.tryLock(); // 无参标识没抢到锁就返回null
        if (!getLock) return Result.fail("一人只能下一单");
        // 获取到锁
        try {
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }*/


}
