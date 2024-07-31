--参数
local voucherId = ARGV[1]
local userId = ARGV[2]
local orderId = ARGV[3]
--key
--lua脚本的字符串拼接是..
local voucherKey = "seckill:stock:"..voucherId
local orderKey = "seckill:order:"..voucherId

--tonumber将得到的字符串数据转化为数字
if(tonumber(redis.call('get', voucherId)) <= 0) then
    return 1
end
if(redis.call('sismember', orderKey, userId)==1) then
    return 2
end
--有购买资格,库存--
redis.call('incrby', voucherId, -1)
--记录购买此券的用户id
redis.call('sadd', orderKey, userId);
--消息发送,相当于生产者创建消息并加到消息队列里面
redis.call('xadd', 'stream.order', '*', 'userId', userId, 'voucherId', voucherId, 'id', orderId)
return 0