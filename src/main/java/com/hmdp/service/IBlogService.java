package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.Blog;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IBlogService extends IService<Blog> {

    /**
     * 点赞数最多的探店博客
     * @param current
     * @return
     */
    Result queryHotBlog(Integer current);


    /**
     * 查看博客详细信息
     * @param id
     * @return
     */
    Result queryBlogById(Long id);

    /**
     * 点赞
     * @param id
     * @return
     */
    Result likeBlog(Long id);

    /**
     * 查询点赞列表
     * @param id
     * @return
     */
    Result queryBlogLikes(Long id);

    /**
     * 保存博客
     * @param blog
     * @return
     */
    Result saveBlog(Blog blog);

    /**
     * 分页查询关注的博主的最新博客
     * @return
     */
    Result queryBlogOfFollow(Long max, Integer offset);

}
