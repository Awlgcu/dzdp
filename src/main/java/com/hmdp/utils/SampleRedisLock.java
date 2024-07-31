package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import io.netty.util.concurrent.UnorderedThreadPoolEventExecutor;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class SampleRedisLock implements Ilock{

    private StringRedisTemplate stringRedisTemplate;
    private String name;
    private static final String keyPrefix = "lock:";
    private static final String idPrefix = UUID.randomUUID().toString(true)+"-";
    //issimple为true是删除uuid之间的-

    private static final DefaultRedisScript<Long> unlockRedis;
    static {
        unlockRedis = new DefaultRedisScript<>();
        unlockRedis.setLocation(new ClassPathResource("unlockRedis.lua"));//指定脚本资源的路径
        unlockRedis.setResultType(Long.class);//指定脚本的返回类型
    }
    //加载脚本,定义一个defaultredisscript类的对象,泛型代表脚本返回值类型,
    //给这个对象指定lua脚本资源,setlocation需要传resource类,这里new一个classpathresource,里面指定resource下面的文件即可


    public SampleRedisLock(StringRedisTemplate stringRedisTemplate, String name) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.name = name;
    }

    @Override
    public boolean tryLock(long expireTimeSec) {
        //获取线程标识,通过uuid和线程id来唯一标识线程
        String threadId = idPrefix+Thread.currentThread().getId();
        //设置互斥锁
        Boolean res = stringRedisTemplate.opsForValue()
                .setIfAbsent(keyPrefix + this.name, threadId, expireTimeSec, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(res);//防止拆箱时空指针!
    }

    @Override
    public void unlock() {
        String key = keyPrefix+this.name;
        String curThreadId = idPrefix+Thread.currentThread().getId();

        //基于lua脚本来释放锁,脚本可以保证判断和释放操作的原子性,不然就算写了先判断再删除也会出现线程安全问题
        stringRedisTemplate.execute(unlockRedis, Collections.singletonList(key), curThreadId);
    }

//    @Override
//    public void unlock() {
//        //先看看锁的val和当前线程对应的val是否一致,一致才能释放锁,防止删除别的线程的锁
//        String threadId = idPrefix+Thread.currentThread().getId();
//        String getId = stringRedisTemplate.opsForValue().get(keyPrefix+this.name);
//        if(threadId.equals(getId)) {
//            stringRedisTemplate.delete(keyPrefix + this.name);
//        }
//    }
}
