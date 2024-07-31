package com.hmdp;

import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import com.hmdp.entity.redisData;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.RedisIdGen;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@SpringBootTest
class HmDianPingApplicationTests {
    @Resource
    private ShopServiceImpl shopService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedisIdGen redisIdGen;

    @Test
    public void test1() throws InterruptedException {
        shopService.Shop2redisData(1L, 10L);
    }
    @Test
    public void test2(){
        LocalDateTime t = LocalDateTime.of(2024, 1, 1, 0, 0, 0);
        long start = t.toEpochSecond(ZoneOffset.UTC);
        System.out.println(start);
    }

    private final ExecutorService es = Executors.newFixedThreadPool(500);//开启500个线程
    @Test
    public void test3() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(300);//多线程计时用的

        Runnable task = ()->{
            for(int i=0;i<100;i++){
                long id = redisIdGen.nextId("order");
                System.out.println(id);
            }
            latch.countDown();
        };
        long begin = System.currentTimeMillis();
        for(int i=0;i<300;i++){
            es.submit(task);//使用300个线程,每个线程生成100个iD
        }
        latch.await();
        long end = System.currentTimeMillis();
        System.out.println("耗时:"+((end-begin)/1000)+"s");
    }

    @Test
    public void test4(){
        redisIdGen.nextId("test");
    }

    @Test
    public void test5(){
        //将数据库中的商户的坐标及id根据商户类型存入redis
        List<Shop> shops = shopService.list();
        //把店铺按照typeid分组放进map,利用了collect下的collectors的分组功能
        Map<Long, List<Shop>> map = shops.stream().collect(Collectors.groupingBy(Shop::getTypeId));
        //分批写入redis
        for(Map.Entry<Long, List<Shop>> entry:map.entrySet()){
            Long typeId = entry.getKey();
            List<Shop> shopList = entry.getValue();
            String key = "shop:geo:"+typeId;
            List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>(shopList.size());
            for (Shop shop : shopList) {
                locations.add(new RedisGeoCommands.GeoLocation<>(shop.getId().toString(), new Point(shop.getX(), shop.getY())));
            }
            //每次添加一个locations集合的数据
            stringRedisTemplate.opsForGeo().add(key, locations);
        }
    }
}
