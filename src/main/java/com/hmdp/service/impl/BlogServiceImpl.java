package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.boot.autoconfigure.security.SecurityProperties;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    @Resource
    private IUserService userService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IFollowService followService;

    @Override
    public Result queryById(Long id) {
        //1.查询博客
        Blog blog = getById(id);
        if(blog==null){
            return Result.fail("笔记不存在");
        }
        //2.根据博客记录的userId查询用户信息
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
        //3.查询blog是否被点赞了
        isBlogLiked(blog);

        return Result.ok(blog);
    }

    private void isBlogLiked(Blog blog) {
        String key = "likedUser:" + blog.getId();
        Long userId = blog.getUserId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score!=null);
    }

    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog ->{
            Long userId = blog.getUserId();
            User user = userService.getById(userId);
            blog.setName(user.getNickName());
            blog.setIcon(user.getIcon());
            //3.查询blog是否被点赞了
            this.isBlogLiked(blog);
        });
        return Result.ok(records);
    }

    @Override
    public Result likeBlog(Long id) {
        String key = "likedUser:" + id;
        //1.判断当前用户是否点赞
        //1.1获取当前用户
        Long userId = UserHolder.getUser().getId();
        //1.2判断是否点赞
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        //利用zset的查score方法判断是否点赞(zset没有ismember),有分数则是点过赞,为null则是没点过赞
        //2.未过赞状态
        if(score==null) {
            //2.1可以点赞,数据库点赞+1
            boolean success = update().setSql("like = like+1").eq("id", id).update();
            if (success) {
                //2.2把用户存到点赞的redis集合
                stringRedisTemplate.opsForZSet().add(key, userId.toString(), System.currentTimeMillis());
                //利用时间作为zset元素的排序分数
            }
        }else {
            //3.已点赞状态
            //3.1取消点赞,数据库点赞-1
            boolean success = update().setSql("like = like-1").eq("id", id).update();
            if(success) {
                //3.2把当前用户点赞信息从redis中移除
                stringRedisTemplate.opsForZSet().remove(key, userId.toString());
            }
        }

        return Result.ok();
    }

    @Override
    public Result getLikeTop5(Long id) {
        String key = "likedUser:" + id;
        //1.获取top5 zrange key 0 4,查到的是用户id,string类型的集合
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        //1.1如果查redis里面一个点赞的用户也没有就返回空集合
        if (top5==null || top5.isEmpty()){
            return Result.ok(Collections.emptyList());
        }
        //2.解析数据得到用户id
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        //3.根据id查询用户并存到集合
        String strIds = StrUtil.join(",", ids);
        List<UserDTO> userDTOS = userService.query().in("id", ids).last("order by field(id,"+strIds+")")
                .list()
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        //4.返回
        return Result.ok(userDTOS);
    }

    @Override
    public Result saveBlog(Blog blog) {
        // 1.获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 2.保存探店博文
        boolean saved = save(blog);
        if(!saved){
            return  Result.fail("保存博客失败");
        }
        //3.保存博客以后,将消息推送到粉丝的收件箱
        //3.1查询所有粉丝先
        List<Follow> fans = followService.query().eq("follow_user_id", user.getId()).list();
        //3.2推送给粉丝
        for (Follow fan : fans) {
            //4.1拿到粉丝id
            Long userId = fan.getUserId();
            //4.2开始推送,推送的是blog的id,用户要自己查?
            String boxKey = "msgBox:"+userId;
            stringRedisTemplate.opsForZSet().add(boxKey, blog.getId().toString(), System.currentTimeMillis());
        }
        // 返回id
        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogofFollow(Long maxTime, Integer offset) {
        //1.获取当前用户
        Long userId = UserHolder.getUser().getId();
        //2.根据当前用户查询消息box
        String boxKey = "msgBox:"+userId;
        Set<ZSetOperations.TypedTuple<String>> blogInfo = stringRedisTemplate.opsForZSet().rangeByScoreWithScores(boxKey, 0, maxTime, offset, 5);
        //3.解析数据:得到blogId, minTime, offset
        if (blogInfo == null || blogInfo.isEmpty()){
            return Result.ok();//消息箱为空,没有消息
        }
        List<Long> ids = new ArrayList<>(blogInfo.size());
        long minTime = 0;
        Integer off_set = 1;
        for (ZSetOperations.TypedTuple<String> info : blogInfo) {
            String blogId = info.getValue();
            ids.add(Long.valueOf(blogId));
            long time = info.getScore().longValue();
            if(time==minTime){
                off_set++;
            }else{
                minTime = time;
                off_set = 1;
            }
        }
        //4.根据bolgId查询blog
        String strIds = StrUtil.join(",", ids);
        List<Blog> blogs = query().in("id", ids).last("order by field(id,"+strIds+")").list();
        //5.因为查询的是blog,需要查询这个blog的用户信息和是否被点赞过
        for (Blog blog : blogs) {
            //2.根据博客记录的userId查询用户信息
            Long user_Id = blog.getUserId();
            User user = userService.getById(user_Id);
            blog.setName(user.getNickName());
            blog.setIcon(user.getIcon());
            //3.查询blog是否被点赞了
            isBlogLiked(blog);
        }
        //6.封装并返回
        ScrollResult scrollResult = new ScrollResult(blogs, minTime, off_set);
        return Result.ok(scrollResult);
    }
}
