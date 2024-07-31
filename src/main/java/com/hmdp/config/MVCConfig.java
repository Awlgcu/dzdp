package com.hmdp.config;

import com.hmdp.utils.loginIntercepter;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
//让拦截器生效的配置类
@Configuration
public class MVCConfig implements WebMvcConfigurer {
    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        //配置我们定义的拦截器
        registry.addInterceptor(new loginIntercepter()).excludePathPatterns(
            "/shop/**",
            "/voucher/**",
            "/shop-type/**",
            "/upload/**",
            "/blog/hot",
            "/user/code",
            "/user/login"
        );
    }
}
