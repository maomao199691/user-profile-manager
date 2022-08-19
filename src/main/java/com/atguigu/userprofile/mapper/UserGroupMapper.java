package com.atguigu.userprofile.mapper;

import com.atguigu.userprofile.bean.UserGroup;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.*;
import org.springframework.stereotype.Service;

import javax.ws.rs.DELETE;
import java.util.List;

@Mapper
@DS("mysql")
public interface UserGroupMapper extends BaseMapper<UserGroup> {

    @Insert("${sql}")
    @DS("clickhouse")
    public void insertUserGroupToClickhouse(String sql);


    /**查询人群包*/
    @Select("select arrayJoin(bitmapToArray(us)) from user_group where user_group_id = #{userGroupId}")
    @DS("clickhouse")
    public List<String> selectBitmapArrayById(String userGroupId);


    /**预估人数*/
    @Select("${bitmapCardiSQL}")
    @DS("clickhouse")
    public Long selectCardinality(String bitmapCardiSQL);


    /**删除clickhouse原有人群包*/
    @Delete("alter table user_group delete where user_group_id = #{userGroupId}")
    @DS("clickhouse")
    public void clearUserGroupUidCk(String userGroupId);

    /**redis单独从clickhouse中查询人群包，不再依赖于clickhouse的查询结果*/
    @Select("${bitmapArraySQL}")
    @DS("clickhouse")
    public List<String> selectBitmapArraySQLById(String bitmapArraySQL);

}
