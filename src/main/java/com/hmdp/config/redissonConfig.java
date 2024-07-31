package com.hmdp.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class redissonConfig {
    @Bean
    public RedissonClient redissonClient(){
        //创建一个redisson的配置,用于创建想要的resissonclient客户端
        Config config = new Config();
        config.useSingleServer().setAddress("redis://192.168.153.129:6379").setPassword("123789");

        return Redisson.create(config);
    }
}
