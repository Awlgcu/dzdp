package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdGen;
import com.hmdp.utils.SampleRedisLock;
import com.hmdp.utils.UserHolder;
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
import java.util.SimpleTimeZone;
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

    @Resource
    private ISeckillVoucherService iSeckillVoucherService;
//    @Resource
//    private IVoucherOrderService voucherOrderService;
    @Resource
    private RedisIdGen redisIdGen;

    @Resource StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> seckillVoucher;
    //要运行脚本要先定义脚本
    static {
        seckillVoucher = new DefaultRedisScript<>();
        seckillVoucher.setLocation(new ClassPathResource("seckillVoucherRedis.lua"));//指定脚本资源的路径
        seckillVoucher.setResultType(Long.class);//指定脚本的返回类型
    }

    private IVoucherOrderService proxy;//定义动态代理对象为成员变量!
    private static  final ExecutorService seckill_executor = Executors.newSingleThreadExecutor();
    //实现将订单信息存入数据库的线程

    @PostConstruct//这个注解的作用是在这个类一创建就会执行这个方法,
    // 这里我们需要的是类一旦创建就用线程去监听阻塞队列,把订单信息存进数据库
    private void init(){
        seckill_executor.submit(new voucherOrderHandler());
    }
//    private BlockingQueue<VoucherOrder> blockingQueue = new ArrayBlockingQueue<>(1024*1024);
    //请求阻塞队列的线程将一直等待阻塞队列出现数据,采用了redis的消息队列实现异步处理,不需要阻塞队列了
    private class voucherOrderHandler implements Runnable{
        private final String streamName = "stream.order";
        //定义线程任务
        @Override
        public void run() {
            while(true){
                try {
//                    VoucherOrder voucherOrder = blockingQueue.take();
                    //不再从jvm阻塞队列获取消息了,去redis消息队列获取信息!
                    //1.判断从消息队列获取消息是否成功
                    List<MapRecord<String, Object, Object>> msg = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),//指定消费者名称和来自哪个组
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),//指定读取方式,每次读一个并且阻塞式读
                            StreamOffset.create(streamName, ReadOffset.lastConsumed())//指定读哪个队列和从哪里读,采取读第一个未读消息的策略
                    );
                    if(msg == null || msg.isEmpty()) {
                        //1.1获取失败,再此尝试
                        continue;
                    }
                    //1.2获取成功,解析消息中的订单信息
                    MapRecord<String, Object, Object> entries = msg.get(0);//得到第一个消息
                    Map<Object, Object> value = entries.getValue();//获取消息中的值,里面有userId, voucherId, orderId
                    //1.3创建订单并且 !ack确认
                    //要将这三个id封装成voucherorder对象即可,这样创建订单可以根据这个对象来创建
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    handleVoucherOrder(voucherOrder);//处理订单
                    //ack确认!
                    stringRedisTemplate.opsForStream().acknowledge(streamName, "g1", entries.getId());

                } catch (Exception e) {
//                    log.error("处理订单异常",e);
                    //出现异常不能直接输出异常了,要读取pending list中的消息,以保证消息都被读取!
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while(true){
                try {
                    //1.从pendinglist里面读数据
                    List<MapRecord<String, Object, Object>> msg = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),//指定消费者名称和来自哪个组
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),//指定读取方式,每次读一个并且阻塞式读
                            StreamOffset.create(streamName, ReadOffset.from("0"))//指定读哪个队列和从哪里读,采取读第一个未读消息的策略
                            //pendinglist的读取从0开始,表示从pendinglist第一个开始读
                    );
                    if(msg == null || msg.isEmpty()) {
                        //1.1获取失败,说明pending list没有消息,即所有消息都确认读了,直接跳出
                        break;
                    }
                    //1.2获取成功,解析消息中的订单信息
                    MapRecord<String, Object, Object> entries = msg.get(0);//得到第一个消息
                    Map<Object, Object> value = entries.getValue();//获取消息中的值,里面有userId, voucherId, orderId
                    //1.3创建订单并且 !ack确认
                    //要将这三个id封装成voucherorder对象即可,这样创建订单可以根据这个对象来创建
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    handleVoucherOrder(voucherOrder);//处理订单
                    //ack确认!
                    stringRedisTemplate.opsForStream().acknowledge(streamName, "g1", entries.getId());
                } catch (Exception e) {
//                   //处理pending list出异常了
                    log.error("处理pendingList出异常");
                    try {
                        Thread.sleep(50); //休眠一会再进行下一次循环
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }

        private void handleVoucherOrder(VoucherOrder voucherOrder) {
                //采用redisson分布式锁
                RLock lock = redissonClient.getLock("order" + voucherOrder.getUserId());//使用redisson的锁
        //        boolean res = lock.tryLock(10);
                boolean res = lock.tryLock();//redisson尝试获取锁的接口,有三个参数,等待时间,锁过期时间,单位timeunit
                //默认值:等待时间-1,表示不等待,过期时间:30,单位:timeunit.seconds,30秒锁自动后过期
                if(!res){
                    log.error("您已下单");
                    return;
                }
                try {
                    //IVoucherOrderService proxy = (IVoucherOrderService)AopContext.currentProxy();//获取动态代理对象!
                    //子线程里面不能获取到代理对象,在主线程提前获取代理对象,子线程可以使用
                    proxy.createVoucherOrder(voucherOrder);
                } finally {
                    lock.unlock();//确保就算获取锁成功后的业务出现异常也要释放锁,不然出现死锁!
                }
            }
    }

    @Override
    public Result seckill(Long voucherId) {//优惠券下单的业务
        //1.执行判断是否有下单资格的lua脚本
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdGen.nextId("order");
        Long res = stringRedisTemplate.execute(seckillVoucher,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), orderId+"");
        int resInt = res.intValue();//将Long型返回值转化为int值
        //2.判断脚本返回结果
        //2.1返回值不为0
        if(resInt==2){
            return Result.fail("您已下过单");
        }else if(resInt==1){
            return Result.fail("库存不足");
        }
        //2.2返回值为0,有购买资格,将订单信息存入阻塞队列,后续开启线程将订单信息存入数据库
//        //2.3获取一个订单信息
//        VoucherOrder voucherOrder = new VoucherOrder();
//        voucherOrder.setUserId(userId);
//        voucherOrder.setVoucherId(voucherId);
//        voucherOrder.setId(orderId);
//        //2.4将这个订单放进阻塞队列
//        blockingQueue.add(voucherOrder);
        //不需要把订单消息存jvm阻塞队列了,采用了redis的消息队列!

        proxy = (IVoucherOrderService)AopContext.currentProxy();//获取动态代理对象!
        //2.5
        //已经开启了线程去执行一个一直监听阻塞队列的方法
        //3.返回订单id
//        long orderId = redisIdGen.nextId("order");
        return Result.ok(orderId);
    }

//    @Override
//    public Result seckill(Long voucherId) {//优惠券下单的业务
//        //1.查询优惠券
//        SeckillVoucher voucher = iSeckillVoucherService.getById(voucherId);
//
//        //2.判断时间是否合理.不合理返回
//        LocalDateTime beginTime = voucher.getBeginTime();
//        if(beginTime.isAfter(LocalDateTime.now())){
//            return Result.fail("秒杀尚未开始");
//        }
//        if(voucher.getEndTime().isBefore(LocalDateTime.now())){
//            return Result.fail("秒杀已经结束");
//        }
//
//        //3.查看库存是否充足,不充足返回
//        Integer num = voucher.getStock();
//        if(num<1){
//            Result.fail("库存不足");
//        }
//        Long userId = UserHolder.getUser().getId();
////        synchronized (userId.toString().intern()){
////            //这里加锁利用userid,这样只有同一个userid的线程才会受到影响,不同userid的线程都可以执行里面的代码
////            //这里用了string的intern方法是为了保证从同一个数值的Long类型变量转换为的字符串对象一定是同一个!
////            //不加intern就算是同一个Long值,由他转化的string对象值也是一样的,但是会被算作不同对象!这样的话这个锁也会失效
//////            return createVoucherOrder(voucherId);
////            //锁只能加到这里,不能加到方法里面,因为整个方法是一个事务,锁放在方法里面,那么存在先开锁
////            //后提交事务的可能,这样在事务提交前同一个用户的id还是可以执行方法!所以必须要先提交事务,再解锁!
////            //是锁包围事务!
////
////            //不能直接调用createVoucherOrder,这样这个方法的@transactional注解会失效
////            //因为spring之所以有事务机制是因为用的当前对象的代理对象去调用的createVoucherOrder方法,
////            //所以要想该方法的事务能生效这里要用当前对象的动态代理去调用这个方法!
////            IVoucherOrderService proxy = (IVoucherOrderService)AopContext.currentProxy();//获取动态代理对象!
////            return proxy.createVoucherOrder(voucherId);
////            //这里需要在接口里面定义一个这个方法,因为创建的是接口的动态代理,而这个方法是写在当前类的,当前类
////            //自己写的这个方法,接口的动态代理没有这个方法,
////            //同时也要引入动态代理的依赖并且在application运行文件上添加一个暴露动态代理对象的注解,不然无法获取动态代理对象
////        }//获取的是jvm的锁,分布式系统或者集群模式下起不了效果
//
//        //基于redis的分布式锁
////        SampleRedisLock lock = new SampleRedisLock(stringRedisTemplate, "order"+userId);
//        //采用redisson分布式锁
//        RLock lock = redissonClient.getLock("order" + userId);//使用redisson的锁
////        boolean res = lock.tryLock(10);
//        boolean res = lock.tryLock();//redisson尝试获取锁的接口,有三个参数,等待时间,锁过期时间,单位timeunit
//        //默认值:等待时间-1,表示不等待,过期时间:30,单位:timeunit.seconds,30秒锁自动后过期
//        if(!res){
//            return Result.fail("您已下单");
//        }
//        try {
//            IVoucherOrderService proxy = (IVoucherOrderService)AopContext.currentProxy();//获取动态代理对象!
//            return proxy.createVoucherOrder(voucherId);
//        } finally {
//            lock.unlock();//确保就算获取锁成功后的业务出现异常也要释放锁,不然出现死锁!
//        }
//    }

    @Transactional
//    @Override
    public void createVoucherOrder(VoucherOrder voucherOrder) {//synchronized相当于加了悲观锁,这个方法只能同时一个线程执行
        //如果对这个方法加synchronized用的是this来加锁,这样每一个线程都会被这个锁影响,最后相当于串行执行,没有意义!

        //plus-检查当前用户是否已经买过券,实现一人一单
        //由于是插入操作,没办法乐观锁来检查是否已经更新,所以只能加悲观锁
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        Integer count = query().eq("voucher_id", voucherId).eq("user_id", userId).count();
        if(count>=1){
            log.error("您已下单");
        }
        //4.库存--
        boolean suc = iSeckillVoucherService.update().setSql("stock=stock-1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0)
                .update();
        //当且仅当stock与我们得到的stock一致时才更新数据库,这样可以解决线程安全问题,相当于加了乐观锁!
        //同时为了解决乐观锁太过小心的缺点,将eq num改为gt 0即可!
        if(!suc){
            log.error("更新库存失败");
        }
        //5.向数据库插入订单信息并返回订单号
        // (订单号不用返回了,返回给前端的时已经被主线程里面的redis干了,现在只需要在数据库存订单信息即可)
        save(voucherOrder);
//        return Result.ok(voucherOrder);
    }
}
