package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import org.apache.ibatis.annotations.Insert;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result sendCode(String phone, HttpSession session) {
        //1.校验手机号
        boolean phoneInvalid = RegexUtils.isPhoneInvalid(phone);
        //注意,这里返回true是不合法,返回false表示合法!
        //不符合返回错误信息
        if(phoneInvalid){
            return Result.fail("手机号错误");
        }
        //符合-生成验证码,保存并返回个前端
        String code = RandomUtil.randomNumbers(6);
        session.setAttribute("code", code);
        log.debug("发送短信验证码成功,验证码:"+code);
        //直接返回即可
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        //校验手机号和验证码,校验失败,返回错误
        String phone = loginForm.getPhone();
        if(RegexUtils.isPhoneInvalid(phone)){
            return Result.fail("手机号错误");
        }
        String verifyCode = loginForm.getCode();
        String cacheCode = (String) session.getAttribute("code");
        if(cacheCode == null || !verifyCode.equals(cacheCode)){
            return Result.fail("验证码错误");
        }

        //校验成功,根据用户是否存在进行操作
        User user = query().eq("phone", phone).one();
        if (user == null) {
            user = createUserWithPhone(phone);
        }
        //校验成功,保存用户信息到session
        session.setAttribute("user", BeanUtil.copyProperties(user, UserDTO.class));
        //利用hutool工具类将user转化为userDTO
        return null;
    }

    @Override
    public Result sign() {
        Long userId = UserHolder.getUser().getId();
        LocalDate now = LocalDate.now();
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = "sign:"+userId+keySuffix;
        int dayOfMonth = now.getDayOfMonth();
        stringRedisTemplate.opsForValue().setBit(key, dayOfMonth-1, true);

        return Result.ok();
    }

    @Override
    public Result countSign() {
        Long userId = UserHolder.getUser().getId();
        LocalDate now = LocalDate.now();
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = "sign:"+userId+keySuffix;
        int dayOfMonth = now.getDayOfMonth();
        List<Long> decimalDays = stringRedisTemplate.opsForValue().bitField(key,
                BitFieldSubCommands.create().get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth))
                        .valueAt(0)
        );//通bitfield拿到从这个月开始到当前为止的签到情况
        if (decimalDays == null ||decimalDays.isEmpty()){
            return Result.ok(0);
        }//如果没拿到,直接返回
        Long dec = decimalDays.get(0);
        if (dec == null|| dec == 0){
            return Result.ok(0);
        }//如果签到天数为0,直接返回
        int count=0;
        while (true){
            if((dec&1)==0){
                break;
            }else{
                count++;
                dec = dec >>> 1;//>>表示带符号的右移,>>>代表无符号右移
            }
        }
        return Result.ok(count);
    }

    private User createUserWithPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName("user_"+RandomUtil.randomString(10));

        save(user);//利用mybatisplus将这个用户保存到数据库!
        return user;
    }
}
