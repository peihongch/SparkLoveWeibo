package entity;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public final class BaseInfo implements Serializable {
    // 官微账号id
    String id;

    // 微博数
    String wb;

    // 粉丝数
    String fans;

    // 被关注数量
    String follows;
}
