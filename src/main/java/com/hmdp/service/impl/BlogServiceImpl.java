package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;

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
    private IBlogService blogService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 点赞数最多的探店博客
     * @param current
     * @return
     */
    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query().orderByDesc("liked").page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            queryBlogUser(blog);
            isBlogLike(blog);
        });
        return Result.ok(records);
    }

    /**
     * 查看博客详细信息
     * @param id
     * @return
     */
    @Override
    public Result queryBlogById(Long id) {
        // 根据博客id查博客
        Blog blog = blogService.getById(id);
        if (blog == null) {
            return Result.fail("为查询到博客");
        }
        // 补全用户信息
        queryBlogUser(blog);
        isBlogLike(blog);
        return Result.ok(blog);
    }

    /**
     * 设置blog中是否已点赞字段的值
     * @param blog
     */
    private void isBlogLike(Blog blog) {
        // 查询该用户是否点过赞
        Long userId = UserHolder.getUser().getId();
        if (userId == null) return;
        // Long userId = UserHolder.getUser().getId();
        String key = BLOG_LIKED_KEY + blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score != null);
    }

    @Override
    public Result likeBlog(Long id) {
        // 查询该用户是否点过赞
        Long userId = UserHolder.getUser().getId();
        // 查询ZSet的score
        Double score = stringRedisTemplate.opsForZSet().score(BLOG_LIKED_KEY + id, userId.toString());
        // 若没点过，赞数+1
        if (score == null) {
            boolean isSuccess = lambdaUpdate().setSql("liked = liked + 1").eq(Blog::getId, id).update();
            if (!isSuccess) return Result.fail("点赞失败");
            // 加入点赞集合
            stringRedisTemplate.opsForZSet().add(BLOG_LIKED_KEY + id, userId.toString(), System.currentTimeMillis());
        } else {
            // 点过赞了
            boolean isSuccess = lambdaUpdate().setSql("liked = liked - 1").eq(Blog::getId, id).update();
            if (!isSuccess) return Result.fail("取消点赞失败");
            // 移除点赞集合
            Long count = stringRedisTemplate.opsForZSet().remove(BLOG_LIKED_KEY + id, userId.toString());
            System.out.println(count);
        }
        return Result.ok();
    }

    /**
     * 查询点赞列表
     * @param id
     * @return
     */
    @Override
    public Result queryBlogLikes(Long id) {
        // 查询top5
        Set<String> top5Ids = stringRedisTemplate.opsForZSet().range(BLOG_LIKED_KEY + id, 0, 4);
        if (top5Ids == null || top5Ids.isEmpty()) return Result.ok(Collections.emptyList());
        // 集合String => Long
        List<Long> userIds = top5Ids.stream().map(Long::valueOf).collect(Collectors.toList());
        String ids = StrUtil.join(",", userIds);
        // 查询用户
        List<UserDTO> UserDTOs = userService.query()
                .in("id", ids).last("order by field (id, " + ids + ")").list()
                .stream().map(user -> BeanUtil.copyProperties(user, UserDTO.class)).collect(Collectors.toList());
        return Result.ok(UserDTOs);
    }

    /**
     * 补充blog中缺少的用户信息
     * @param blog
     */
    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setIcon(user.getIcon());
        blog.setName(user.getNickName());
    }
}
