package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result shopList() {
        String key = "cache:shopType";
        List<String> sList = stringRedisTemplate.opsForList().range(key, 0, -1);
        if(sList!=null&&!sList.isEmpty()){
            List<ShopType> bean = new ArrayList<>();
            for (String s : sList) {
                ShopType b = JSONUtil.toBean(s, ShopType.class);
                bean.add(b);
            }
            return Result.ok(bean);
        }
        //mybatis-plus
        List<ShopType> typeList = query().orderByAsc("sort").list();

        if(typeList == null){
            return Result.fail("未找到任何店铺信息");
        }
        for (ShopType it : typeList) {
            String v = JSONUtil.toJsonStr(it);
            stringRedisTemplate.opsForList().rightPush(key, v);
        }

//        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(typeList));
        return Result.ok(typeList);
    }
}
