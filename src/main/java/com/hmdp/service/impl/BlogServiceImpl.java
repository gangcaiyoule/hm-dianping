package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
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

    private void isBlogLike(Blog blog) {
        // 查询该用户是否点过赞
        Long userId = UserHolder.getUser().getId();
        String key = "blog:like:" + blog.getId();
        Boolean isMember = stringRedisTemplate.opsForSet().isMember(key, userId.toString());
        blog.setIsLike(BooleanUtil.isTrue(isMember));
    }

    @Override
    public Result likeBlog(Long id) {
        // 查询该用户是否点过赞
        Long userId = UserHolder.getUser().getId();
        String key = "blog:like:" + id;
        Boolean isMember = stringRedisTemplate.opsForSet().isMember(key, userId.toString());
        // 若没点过，赞数+1
        if (BooleanUtil.isFalse(isMember)) {
            boolean isSuccess = blogService.update().setSql("liked = liked + 1").eq("user_id", userId).update();
            if (!isSuccess) return Result.fail("点赞失败");
            // 加入点赞集合
            stringRedisTemplate.opsForSet().add(key, userId.toString());
        } else {
            // 点过赞了
            boolean isSuccess = blogService.update().setSql("liked = liked - 1").eq("user_id", userId).update();
            if (!isSuccess) return Result.fail("取消点赞失败");
            // 移除点赞集合
            stringRedisTemplate.opsForSet().remove(key, userId.toString());
        }
        return Result.ok();
    }

    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setIcon(user.getIcon());
        blog.setName(user.getNickName());
    }
}
