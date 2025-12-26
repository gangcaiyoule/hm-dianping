-- 参数
local voucherId = ARGV[1]
local userId = ARGV[2]
local orderId = ARGV[3]

-- redis key
local stockKey = "seckill:stock:" .. voucherId
local orderKey = "seckill:user:" .. voucherId

-- 业务
-- 检测库存是否充足
if tonumber(redis.call('get', stockKey)) <= 0 then
    return 1
end
-- 检测用户是否下过单
if redis.call('sismember', orderKey, userId) == 1 then
    return 2
end
-- 减库存
redis.call('incrby', stockKey, -1)
-- 保存用户
redis.call('sadd', orderKey, userId)
-- 发消息
redis.call('xadd', 'stream:orders', '*', 'userId', userId, 'voucherId', voucherId, 'id', orderId)
return 0