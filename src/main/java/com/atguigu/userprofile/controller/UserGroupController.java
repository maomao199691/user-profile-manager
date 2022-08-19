package com.atguigu.userprofile.controller;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.userprofile.bean.TagCondition;
import com.atguigu.userprofile.bean.TaskInfo;
import com.atguigu.userprofile.bean.UserGroup;
import com.atguigu.userprofile.service.UserGroupService;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import io.swagger.annotations.ApiOperation;
import org.apache.catalina.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.ws.rs.POST;
import java.util.Date;
import java.util.List;

@RestController
public class UserGroupController {

    @Autowired
    UserGroupService userGroupService;

    @RequestMapping("/user-group-list")
    @CrossOrigin
    public String  getUserGroupList(@RequestParam("pageNo")int pageNo , @RequestParam("pageSize") int pageSize){
        int startNo=(  pageNo-1)* pageSize;
        int endNo=startNo+pageSize;

        QueryWrapper<UserGroup> queryWrapper = new QueryWrapper<>();
        int count = userGroupService.count(queryWrapper);

        queryWrapper.orderByDesc("id").last(" limit " + startNo + "," + endNo);
        List<UserGroup> userGroupList = userGroupService.list(queryWrapper);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("detail",userGroupList);
        jsonObject.put("total",count);

        return  jsonObject.toJSONString();
    }


    //接收请求
    //调用服务层
    //返回结果
    /**
     * 创建分群：
     *  1.将分群基本信息存储到mysql中
     *  2.生成人群包，由clickhouse负责计算
     *  3.将生成的人群包信息转存到redis中，用于支持高QPS的应用
     *
     */
    @PostMapping("/user-group")
    public  String  saveUserGroup(@RequestBody  UserGroup userGroup){
        //1.保存分群信息
        this.userGroupService.saveUserGroupInfos(userGroup);

        //2计算出该分群的用户群体（分群包），uid 的集合,由clickhouse负责，通过 bitmap 的计算完成
        this.userGroupService.genUserGroupUids(userGroup);

        //3.把计算的结果保存你在可以处理高 QPS 的数据容器中， redis、mysql等
        userGroupService.saveUserGroupIdToRedis(userGroup);

        //更新人数
        Long evaluateNum = userGroupService.evaluateUserGroupNums(userGroup);
        userGroup.setUserGroupNum(evaluateNum);
        userGroupService.saveOrUpdate(userGroup);

        return "success";
    }

    /**预估人数*/
    @PostMapping("/user-group-evaluate")
    public Long userGroupEualuate(@RequestBody UserGroup userGroup){

        return  userGroupService.evaluateUserGroupNums(userGroup);
    }


    /** 更新分群*/
    @PostMapping("/user-group-refresh/{userGroupId}")
    public String userGroupRefresh(@PathVariable("userGroupId") String userGroupId ,@RequestParam("busiDate") String busiDate ){
        //1.删除原有的人群包(ck,redis)
        userGroupService.clearUserGroupUidCk(userGroupId);
        userGroupService.clearUserGroupUidRedis(userGroupId);

        //2.重新生成人群包
        UserGroup usergroup = userGroupService.getById(userGroupId);

        // 补充 userGroup 信息
        usergroup.setTagConditions(JSON.parseArray(usergroup.getConditionJsonStr(),TagCondition.class));
        usergroup.setBusiDate(busiDate);

        userGroupService.genUserGroupUids(usergroup);
        userGroupService.saveUserGroupIdToRedis(usergroup);

        //3.修改基本信息
        Long evaluateNum = userGroupService.evaluateUserGroupNums(usergroup);
        usergroup.setUserGroupNum(evaluateNum);
        usergroup.setUpdateTime(new Date());

        userGroupService.saveOrUpdate(usergroup);

        return "success";
    }

}

