package entity;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class WeiboItem {
    // 微博id
    String id;

    // 如果非null，则该条微博是转发的，respost是原微博作者
    String repost;

    // 微博内容
    String content;

    // 微博标签
    List<String> tag;

    // 微博@
    List<String> at;

    // 配图数
    String imgNum;

    // 点赞数量
    String likeNum;

    // 转发数量
    String respostNum;

    // 评论数量
    String commentNum;

    // 发微博时间
    String time;
}
