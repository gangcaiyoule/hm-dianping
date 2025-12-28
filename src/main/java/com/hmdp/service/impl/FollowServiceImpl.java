package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
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
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.FOLLOWS_KEY;

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

    /**
     * 关注按钮
     * @param followUserId
     * @param isFollow
     * @return
     */
    @Override
    public Result follow(Long followUserId, boolean isFollow) {
        Long userId = UserHolder.getUser().getId();
        String key = FOLLOWS_KEY + userId;
        // 查询关注情况
        if (isFollow) {
            // 关注
            Follow follow = Follow.builder()
                    .userId(userId)
                    .followUserId(followUserId)
                    .createTime(LocalDateTime.now())
                    .build();
            boolean isSuccess = save(follow);
            // redis也放一份
            if (isSuccess) stringRedisTemplate.opsForSet().add(key, followUserId.toString());
        } else {
            // 取关
            LambdaQueryWrapper<Follow> wrapper = new LambdaQueryWrapper<>();
            wrapper.eq(Follow::getUserId, userId).eq(Follow::getFollowUserId, followUserId);
            boolean isSuccess = remove(wrapper);
            // redis也要删除
            if (isSuccess) stringRedisTemplate.opsForSet().remove(key, followUserId);
        }
        return Result.ok(followUserId);
    }

    /**
     * 查询是否关注
     * @param followUserId
     * @return
     */
    @Override
    public Result isFollow(Long followUserId) {
        Long userId = UserHolder.getUser().getId();
        Integer count = lambdaQuery().eq(Follow::getFollowUserId, followUserId).eq(Follow::getUserId, userId).count();
        return Result.ok(count > 0);
    }

    /**
     * 共同关注
     * @param userId
     * @return
     */
    @Override
    public Result common(Long userId) {
        // 设置key
        Long currentUserId = UserHolder.getUser().getId();
        String targetUserKey = FOLLOWS_KEY + userId;
        String currentUserKey = FOLLOWS_KEY + currentUserId;
        // 交集
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(targetUserKey, currentUserKey);
        if (intersect == null || intersect.isEmpty()) return Result.ok(Collections.emptyList());
        // 获取Long型id
        List<Long> ids = intersect.stream().map(Long::parseLong).collect(Collectors.toList());
        // 查询User信息
        List<User> users = userService.listByIds(ids);
        // User -> UsereDTO
        List<UserDTO> userDTOS = users.stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(userDTOS);
    }
}
