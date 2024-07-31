package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.entity.redisData;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
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

    @Override
    public Result queryById(Long id) {
        Shop sp = null;
        //1.解决缓存穿透的访问
//        sp = getByIdWithPassThrough(id);
        sp = cacheClient.getByIdWithPassThrough("cache:shop:", id, Shop.class, this::getById, 300L, TimeUnit.SECONDS);

        //2.解决缓存击穿的访问--互斥锁实现
//        sp = getByIdWithMutex(id);

        //2.解决缓存击穿的访问--逻辑过期实现
//        sp = getByIdWithLogicalExpire(id);
//        sp = cacheClient.getByIdWithLogicalExpire("cache:shop:", id, Shop.class, this::getById, 24L, TimeUnit.SECONDS);

        if(sp == null){
            return Result.fail("店铺不存在");
        }
        return Result.ok(sp);
    }


    private boolean tryLock(String key){ //获取锁
        Boolean b = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        //防止空指针异常,不直接return而是使用booleanutil工具类
        return BooleanUtil.isTrue(b);
    }
    private void unLock(String key){ //释放锁
        stringRedisTemplate.delete(key);
    }
//应对缓存击穿的查询-互斥锁实现
    private Shop getByIdWithMutex(Long id){
        String key = "cache:shop:" + id;
        //1.从redis查
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断redis中是否存在//3.存在则返回
        if(!StrUtil.isBlank(shopJson)){
            if(shopJson.equals("null")){
                return null;
            }
            Shop sp = JSONUtil.toBean(shopJson, Shop.class);
            return sp;
        }
        //4.不存在
        //4.1先获取锁,每能获取锁则等待后重新访问
        String lockKey = null;
        Shop sp = null;
        try {
            lockKey = "cache:lock:shop:"+id;
            boolean res = tryLock(lockKey);
            if(!res){
                Thread.sleep(100);
                return getByIdWithMutex(id);
            }
            //4.2成功获取锁线程的去重建数据
            sp = getById(id);
//----------------------------------------------------------------------------------------------
            //模拟阻塞
            Thread.sleep(200);
//----------------------------------------------------------------------------------------------
            //5.数据库也不存在
            if (sp == null) {
                stringRedisTemplate.opsForValue().set(key, "null", 2L, TimeUnit.MINUTES);
                return null;
            }
            //6.存在,先把数据写进redis,再返回
            //同时设置过期时间为24h
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(sp), 24L, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            //7.释放锁
            unLock(lockKey);
        }
        return sp;
    }
////应对缓存穿透的查询--逻辑过期实现
//    private static final ExecutorService CacheRebuildExecutor = Executors.newFixedThreadPool(10);
//    //最多10个线程
//    private Shop getByIdWithLogicalExpire(Long id){
//        String key = "cache:redisData:" + id;
//        //1.从redis查
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        //2.判断redis中是否存在,没查到,返回null
//        if(StrUtil.isBlank(shopJson)){
//            return null;
//        }
//        //3.查到了,检查是否过期
//        redisData redisdata = JSONUtil.toBean(shopJson, redisData.class);
//        //因为redisdata里面的data是object不是shop,所以要先转为jsonobject再用jsonutils转为shop
//        JSONObject jsonData = (JSONObject) redisdata.getData();
//        Shop shop = JSONUtil.toBean(jsonData, Shop.class);
//        LocalDateTime expireTime = redisdata.getExpireTime();
//        //3.1 没过期,直接返回
//        if(expireTime.isAfter(LocalDateTime.now())){
//            return shop;
//        }
//        //3.2 过期
//        //4.也返回,获取锁并根据锁更新缓存
//        //只有成功时才更新,但是不管成功与否都要返回旧值
//        String lockKey = "cache:lock:"+id;
//        if (tryLock(lockKey)) {
//           //4.1获取到了,则开启新线程去更新,然后同时返回旧值
//            CacheRebuildExecutor.submit(
//                    ()->{
//                        try {
//                            //重建缓存
//                            this.Shop2redisData(id, 20L);
//                        } catch (Exception e) {
//                            throw new RuntimeException(e);
//                        }finally {
//                            //释放锁
//                            unLock(lockKey);
//                        }
//                    }
//            );
//        }
//        //不管获取锁成功与否都要返回旧值
//        return shop;
//    }
////应对缓存穿透的查询
//    private Shop getByIdWithPassThrough(Long id){
//        String key = "cache:shop:" + id;
//        //1.从redis查
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//
//        //2.判断redis中是否存在//3.存在则返回
//        if(!StrUtil.isBlank(shopJson)){
//            if(shopJson.equals("null")){
//                return null;
//            }
//            Shop sp = JSONUtil.toBean(shopJson, Shop.class);
//            return sp;
//        }
//
//        //4.不存在先根据id查数据库(mybatis-plus)
//        Shop sp = getById(id);
//
//        //5.数据库也不存在,返回404
//        if (sp == null) {
//            stringRedisTemplate.opsForValue().set(key, "null", 2L, TimeUnit.MINUTES);
//            return null;
//        }
//        //6.存在,先把数据写进redis,再返回
//        //同时设置过期时间为24h
//        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(sp), 24L, TimeUnit.HOURS);
//
//        return sp;
//    }

    @Override
    @Transactional //单体项目,可以根据事务控制更新数据库且更新缓存的原子性
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id == null){
            return Result.fail("id不存在");
        }
        updateById(shop);
        stringRedisTemplate.delete("cache:shop:"+id);
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        //1.判断是否需要根据坐标查询
        if(x == null || y == null){
            // 不需要根据坐标查,返回数据库结果
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            return Result.ok(page.getRecords());
        }

        //2.计算分页参数
        int from = (current-1)*SystemConstants.DEFAULT_PAGE_SIZE;
        int end = (current)*SystemConstants.DEFAULT_PAGE_SIZE;
        //3.查询redis,按照距离排序,分页 结果:shopId, distance
        String key = "shop:geo:"+typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo().search(
                key,
                GeoReference.fromCoordinate(new Point(x, y)),//以当前坐标为参考
                new Distance(5000),//搜索范围,5km
                RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                //这里分页只能从0-end,要自己实现from-end
        );

        //4.解析结果,获得shopId并据此查询shop
        if (results == null){
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> contents = results.getContent();
        List<Long> ids = new ArrayList<>(contents.size());
        Map<String, Distance> mp = new HashMap<>(contents.size());

        //如果查询到的店铺数量小于此次查询应该跳过的数量了,不用也不能执行下面的逻辑了
        if (contents.size()<=from){
            return Result.ok(Collections.emptyList());
        }

        contents.stream().skip(from).forEach(res -> {
            String shopIdStr = res.getContent().getName();//得到店铺id
            Distance distance = res.getDistance();//得到店铺距离当前位置的距离

            ids.add(Long.valueOf(shopIdStr));
            mp.put(shopIdStr, distance);
        });
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("order by field (id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(mp.get(shop.getId().toString()).getValue());
            //店铺有一个参数distance,专门用于现在记录店铺距离当前用户的位置!直接set就可以了
        }
        // 5.返回数据
        return Result.ok(shops);
    }

    //一个将shop封装成带过期时间的方法
    public void Shop2redisData(Long id, Long et) throws InterruptedException {
        Shop shop = getById(id);
        Thread.sleep(200);//模拟重建热点key的耗时
        redisData res = new redisData();
        res.setData(shop);
        res.setExpireTime(LocalDateTime.now().plusSeconds(et));
        String jsonStr = JSONUtil.toJsonStr(res);
        stringRedisTemplate.opsForValue().set("cache:shop:"+id, jsonStr);
    }
}
