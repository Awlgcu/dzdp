package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import com.hmdp.entity.redisData;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Component
@Slf4j
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate){
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicExpire(String key, Object value, Long time, TimeUnit unit){
        RedisData redisData = new RedisData();
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        redisData.setData(value);
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    //应对缓存穿透的查询
    public  <Re, ID> Re getByIdWithPassThrough(String keyPrefix, ID id, Class<Re> type, Function<ID, Re> dbFallback, Long time, TimeUnit unit){
        String key = keyPrefix + id;
        //1.从redis查
        String json = stringRedisTemplate.opsForValue().get(key);

        //2.判断redis中是否存在//3.存在则返回
        if(!StrUtil.isBlank(json)){
            if(json.equals("null")){
                return null;
            }
            return JSONUtil.toBean(json, type);//返回的类型就是传入的type
        }

        //4.不存在先根据id查数据库(mybatis-plus)
        //这段逻辑因为我们不知道用户要查什么类型的数据,所以需要用户传入这一段函数式编程
        Re re = dbFallback.apply(id);

        //5.数据库也不存在,返回404
        if (re == null) {
            stringRedisTemplate.opsForValue().set(key, "null", 2L, TimeUnit.MINUTES);
            return null;
        }
        //6.存在,先把数据写进redis,再返回
        //用户自己决定过期时间
        this.set(key, re, time, unit);

        return re;
    }

    //应对缓存穿透的查询--逻辑过期实现
    private static final ExecutorService CacheRebuildExecutor = Executors.newFixedThreadPool(10);
    //最多10个线程
    public <Re, ID> Re getByIdWithLogicalExpire(String keyPrefix, ID id, Class<Re> type, Function<ID, Re> dbFallback, Long time, TimeUnit unit){
        String key = keyPrefix + id;
        //1.从redis查
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.判断redis中是否存在,没查到,返回null
        if(StrUtil.isBlank(json)){
            return null;
        }
        //3.查到了,检查是否过期
        redisData redisdata = JSONUtil.toBean(json, redisData.class);
        //因为redisdata里面的data是object不是shop,所以要先转为jsonobject再用jsonutils转为shop
        JSONObject jsonData = (JSONObject) redisdata.getData();
        Re re = JSONUtil.toBean(jsonData, type);
        LocalDateTime expireTime = redisdata.getExpireTime();
        //3.1 没过期,直接返回
        if(expireTime.isAfter(LocalDateTime.now())){
            return re;
        }
        //3.2 过期
        //4.也返回,获取锁并根据锁更新缓存
        //只有成功时才更新,但是不管成功与否都要返回旧值
        String lockKey = "cache:lock:"+id;
        if (tryLock(lockKey)) {
            //4.1获取到了,则开启新线程去更新,然后同时返回旧值
            CacheRebuildExecutor.submit(
                    ()->{
                        try {
                            //重建缓存
                            //查数据库
                            Re r = dbFallback.apply(id);

                            //写入缓存
                            this.setWithLogicExpire(key, r, time, unit);

                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }finally {
                            //释放锁
                            unLock(lockKey);
                        }
                    }
            );
        }
        //不管获取锁成功与否都要返回旧值
        return re;
    }

    private boolean tryLock(String key){ //获取锁
        Boolean b = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        //防止空指针异常,不直接return而是使用booleanutil工具类
        return BooleanUtil.isTrue(b);
    }
    private void unLock(String key){ //释放锁
        stringRedisTemplate.delete(key);
    }


//    //一个将shop封装成带过期时间的方法
//    public void Shop2redisData(Long id, Long et) throws InterruptedException {
//        Shop shop = getById(id);
//        Thread.sleep(200);//模拟重建热点key的耗时
//        redisData res = new redisData();
//        res.setData(shop);
//        res.setExpireTime(LocalDateTime.now().plusSeconds(et));
//        String jsonStr = JSONUtil.toJsonStr(res);
//        stringRedisTemplate.opsForValue().set("cache:redisData:"+id, jsonStr);
//
//    }

}
