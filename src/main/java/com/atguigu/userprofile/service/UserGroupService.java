package com.atguigu.userprofile.service;

import com.atguigu.userprofile.bean.UserGroup;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.extension.service.IService;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

public interface UserGroupService  extends IService<UserGroup> {


    public void saveUserGroupInfos(UserGroup userGroup);

    public  void genUserGroupUids(UserGroup userGroup);

    //转存到 redis 中
    public void saveUserGroupIdToRedis(UserGroup userGroup);

    //预估人群
    public Long evaluateUserGroupNums(UserGroup userGroup);

    //删除clickhouse原有人群包
    public void clearUserGroupUidCk(String userGroupId);

    //删除redis原有人群包
    public void clearUserGroupUidRedis(String userGroupId);

}
