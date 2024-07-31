package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdGen {
    long startTimeStamp = 1704067200; //2024-1-1-0-0-0
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    public long nextId (String preFix){
        //生成时间戳-31位
        long timeStamp = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC) - startTimeStamp;
        //生成序列号-32位
        //先生成当前日期加在key后面,防止所有的序列号都用同一个key,超过2^32的redis存储上限
        String date = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        long increment = stringRedisTemplate.opsForValue().increment("inc:" + preFix + ":" + date);
//        System.out.println(increment);
        //拼接并返回,需要利用位运算
        return timeStamp << 32 | increment; //timestamp左移32位把空间留出来给序列号,再或运算就算把序列号拼上去了
    }
}
