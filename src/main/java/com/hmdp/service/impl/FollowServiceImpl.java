package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.conditions.query.QueryChainWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.apache.ibatis.annotations.Delete;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IUserService userService;
    @Override
    public Result follow(Long upId, boolean followed) {
        Long userId = UserHolder.getUser().getId();
        String key  = "user-follows:"+userId;
        //1.判断是否关注
        if(!followed) {
            //2.未关注,关注,即新增数据
            Follow follow = new Follow();
            follow.setUserId(userId);
            follow.setFollowUserId(upId);
            boolean save = save(follow);
            //3.把关注信息存到redis以便后续查询共同关注
            if(save) {
                stringRedisTemplate.opsForSet().add(key, upId.toString());
            }
        }else {
            //3.已关注,取关,即删除数据
            boolean remove = remove(new QueryWrapper<Follow>()
                    .eq("user_id", userId)
                    .eq("follow_user_id", upId)
            );
            if (remove){
                stringRedisTemplate.opsForSet().remove(key, upId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result iSFollowed(Long upId) {
        Long userId = UserHolder.getUser().getId();
        Integer count = query().eq("user_id", userId).eq("followed_user_id", upId).count();
        //判断是否存在的mybatisplus sql查询,加上count关键字判断返回值是否为0即可
        return Result.ok(count>0);
    }

    @Override
    public Result commonFollow(Long upId) {
        Long userId = UserHolder.getUser().getId();
        String key1 = "user-follows"+userId;
        String key2 = "user-follows"+upId;
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(key1, key2);
        if(intersect ==null||intersect.isEmpty()){
            return Result.ok(Collections.emptyList());
        }
        List<Long> interIds = intersect.stream().map(Long::valueOf).collect(Collectors.toList());
        List<UserDTO> commonUsers = userService.listByIds(interIds).stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(commonUsers);
    }
}
