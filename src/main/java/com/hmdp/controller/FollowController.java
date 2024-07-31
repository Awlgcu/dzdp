package com.hmdp.controller;


import com.hmdp.dto.Result;
import com.hmdp.service.IFollowService;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@RestController
@RequestMapping("/follow")
public class FollowController {
    @Resource
    private IFollowService followService;

    @PutMapping("/{upId}/{followed}")
    public Result follow(@PathVariable("upId")Long upId, @PathVariable("followed")boolean followed){
        return followService.follow(upId, followed);
    }

    @GetMapping("/or/not/{upId}")
    public Result isFollowed(@PathVariable("upId") Long upId){
        return followService.iSFollowed(upId);
    }

    @GetMapping("/follow/common/{id}")
    public Result commonFollow(@PathVariable("id") Long upId){
        return followService.commonFollow(upId);
    }

}
