package com.atguigu.userprofile.service.impl;

import com.alibaba.fastjson.JSON;
import com.atguigu.userprofile.bean.TagCondition;
import com.atguigu.userprofile.bean.TagInfo;
import com.atguigu.userprofile.bean.UserGroup;
import com.atguigu.userprofile.constants.ConstCodes;
import com.atguigu.userprofile.mapper.UserGroupMapper;
import com.atguigu.userprofile.service.TagInfoService;
import com.atguigu.userprofile.service.UserGroupService;
import com.atguigu.userprofile.utils.RedisUtil;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;

import org.apache.catalina.User;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;

import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
@DS("mysql")
public class UserGroupServiceImpl extends ServiceImpl<UserGroupMapper, UserGroup> implements UserGroupService {

    @Autowired
    TagInfoService tagInfoService;

    /**
     * 用于将分群信息转存到mysql中
     *
     * 1.将分群条件 tagConditions 转换成json字符串格式，赋值给conditionJsonStr，方便在mysql中存储
     * 2.将分群条件 tagConditions 转化成中文描述，赋值给conditionComment，目的视为了方便在页面中人为的读取
     * 3.补充创建时间 createTime
     */
    @Override
    public void saveUserGroupInfos(UserGroup userGroup) {

        //1.将传入的json信息List集合，转换成json字符串，赋值给conditionJsonStr
        userGroup.setConditionJsonStr(JSON.toJSONString(userGroup.getTagConditions()));

        //2.将userGroup中tagConditions的tagName、operatorName、tagValues进行拼接，转换成中文描述（直接调用UserGroup中定义的方法）
        userGroup.setConditionComment(userGroup.conditionJsonToComment());

        //3.给UserGroup中的createTime赋值，赋值当前时间
        userGroup.setCreateTime(new Date());

        //将处理过的UserGroup存储到mysql
        this.saveOrUpdate(userGroup);

    }

    /**
     * 生成人群包
     *   1.按照分群条件组织sq到clickhouse中查询，得到人群包（uids），并将人群包存储到表中
     *
     *   SQL: 单个条件select => bitmapAnd => select => insert
     */
    @Override
    public void genUserGroupUids(UserGroup userGroup) {

        //调用genInsertSql方法，获取插入语句
        String insertSql = genInsertSql(userGroup);

        this.baseMapper.insertUserGroupToClickhouse(insertSql);

    }

    //转存人群包到 redis 中
    @Override
    public void saveUserGroupIdToRedis(UserGroup userGroup) {

        /**
         * 将Clickhouse中生成的人群包转存到Redis中
         *
         * 1、从Clickhouse中查询人群包
         *
         *
         * Type:    set
         * key:     usergroup:[分群id]
         * value:   uid的集合
         * 写入API:   sadd
         * 读取API:   smembers 、 sismember
         * 是否过期: 不过期
         */

        //1.根据主键查询 clickhouse 的人群包结果
        Long userGroupId = userGroup.getId();
        //List<String> uidList = this.baseMapper.selectBitmapArrayById(userGroupId.toString());
        //System.out.println("uidLise : " + uidList);

        //TODO: 更新人群包的时候，如何防止查询出旧数据
        // 1.等待几秒，负面效果是影响用户体验，无法确定等待几秒
        // 2.optimize table xxx final (老版本可以（同步），新版本不行（异步）)，会影响 clickhouse 的性能
        // 3.解耦 redis 的执行效果不依赖 clickhouse 的计算结果

        String bitmapSql = genBitmapAndSql(userGroup.getTagConditions(), userGroup.getBusiDate());
        String selectSql = "";
        if (userGroup.getTagConditions().size() == 1){
            selectSql = " select arrayJoin( bitmapToArray(bts.us )) from ( " + bitmapSql + " ) bts";
        }else{
            selectSql = " select arrayJoin( bitmapToArray( " + bitmapSql + " ))";
        }
        System.out.println("select: " + selectSql);

        List<String> uidList = this.baseMapper.selectBitmapArraySQLById(selectSql);

        //2.写入 Redis
        Jedis jedis = RedisUtil.getJedis();
        String userGroupKey = "user:group:" + userGroupId;
        jedis.sadd(userGroupKey, uidList.toArray(new String[]{}));
        jedis.close();

    }

    /**预估人群数*/
    @Override
    public Long evaluateUserGroupNums(UserGroup userGroup) {
        String bitmapAndSql = genBitmapAndSql(userGroup.getTagConditions(), userGroup.getBusiDate());
        String selectSql = null;
        if (userGroup.getTagConditions().size() == 1){
            selectSql= " select bitmapCardinality(bts.us) from (" + bitmapAndSql + ") bts";
        }else{
            selectSql = " select bitmapCardinality( " + bitmapAndSql + " )";
        }
        System.out.println("selectSql : " + selectSql);

        return this.baseMapper.selectCardinality(selectSql);
    }

    /** 删除clickhouse上原有的人群包 */
    @Override
    public void clearUserGroupUidCk(String userGroupId) {
        this.baseMapper.clearUserGroupUidCk(userGroupId);
    }

    /** 更新分群，删除 redis中的人群包*/
    @Override
    public void clearUserGroupUidRedis(String userGroupId) {
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "user:group:" + userGroupId;
        jedis.del(redisKey);
        jedis.close();
    }


    private String genInsertSql(UserGroup userGroup){
        //查询Sql
        String select = genSelectSql(userGroup.getId().toString(), userGroup.getTagConditions(), userGroup.getBusiDate());
        //插入Sql
        String insertSql = "insert into user_group " + select;

        return insertSql;
    }

    /** 处理使用了bitmapAndSql和没有使用bitmapAndSql的子查询 ， 得到最终查询语句*/
    private String genSelectSql(String userGroupId, List<TagCondition> tagConditions, String subiDate){
        String bitmapAndSql = genBitmapAndSql(tagConditions, subiDate);
        String selectSql = "";
        if (tagConditions.size() > 1){
            //多值查询
            selectSql = "select '" + userGroupId + "' , " + bitmapAndSql + " , '" + subiDate + "'";
        }else {
            //单值查询 bitmapAndSql = select groupBitmapMergeState(us)
            selectSql= "select '" + userGroupId + "' , (" + bitmapAndSql + ") , '" + subiDate + "'";
        }
        return selectSql;
    }

    /**
     * 将子查询组合到一起，使用 bitmapAnd 将多个子串union到一起
     */
    private String genBitmapAndSql(List<TagCondition> tagConditions, String busiDate){
        //TagInfo组成的Map集合
        Map<String, TagInfo> tagInfoMap = tagInfoService.getTagInfoMapWithCode();
        //字符串缓冲流
        StringBuilder sbs = new StringBuilder();
        //bitmapAnd((bitmapAnd((sub1),(sub2))),(sub3))
        for (int i = 0; i < tagConditions.size(); i++) {
            TagCondition tagCondition = tagConditions.get(i);
            String subQuerySql = genSubQueryTagCondition(tagCondition, busiDate, tagInfoMap);
            if (i == 0){
                sbs.append( subQuerySql );
            }else{
                sbs.insert(0, "bitmapAnd(( ").append(" ),( ").append( subQuerySql ).append(" ))");
            }
        }
        return sbs.toString();
    }


    /**
     *  人群包-子查询方法
     *
     *  待解决问题：
     *    1. 确定表 | user_tag_value_date | user_tag_value_decimal | user_tag_value_long | user_tag_value_string
     *    2.tag_code = ?
     *    3.tag_value 后面的操作符如何确定？ = > < in notin
     *    4.确定tag_value是否要加单引号 、 小括号
     */
    public String genSubQueryTagCondition(TagCondition tagCondition, String busiDate, Map<String ,TagInfo> tagInfoMap){

        //1.通过tagCondition中的tagCode 对应 TagInfo 表中的 tag_value_type ,找到要查询的目标表
        String tagCode = tagCondition.getTagCode();
        //在 TagInfoServiceImpl 中定义的方法，将 TaInfo 中的 tag_code 和 TagInfo 所有字段映射为Map，=> Map<TaInfo.tag_code, TaInfo>
        TagInfo tagInfo = tagInfoMap.get(tagCode);
        //通过map键值对，找到相对应的 tag_value_type
        String tagValueType = tagInfo.getTagValueType();
        //子查询 要查询的目标表
        String selectTableName = null;

        //用来判断是否要加单引号
        Boolean singleQuotes = false;
        if (ConstCodes.TAG_VALUE_TYPE_STRING.equals(tagValueType)){
            selectTableName = "user_tag_value_string";
            singleQuotes = true;
        }else
        if (ConstCodes.TAG_VALUE_TYPE_LONG.equals(tagValueType)){
            selectTableName = "user_tag_value_long";
        }else
        if (ConstCodes.TAG_VALUE_TYPE_DECIMAL.equals(tagValueType)){
            selectTableName = "user_tag_value_decimal";
        }else
        if (ConstCodes.TAG_VALUE_TYPE_DATE.equals(tagValueType)){
            selectTableName = "user_tag_value_date";
            singleQuotes = true;
        }

        //2.过滤条件tag_code
        String selectTagCode = tagCode.toLowerCase();

        //3.确定tag_value后面的操作符
        String selectOperator = getConditionOperator(tagCondition.getOperator());

        //4.确定tag_values是否要加单引号 、小括号
        String selectTagValues = null;
        List<String> tagValues = tagCondition.getTagValues();

        //判断是否加单引
        if (singleQuotes){
            selectTagValues = "'" + StringUtils.join(tagValues,"','") + "'";
        }else{
            selectTagValues = StringUtils.join(tagValues,",");
        }

        //判断是否加小括号
        if (tagCondition.getOperator().equals("in") || tagCondition.getOperator().equals("nin")){
            selectTagValues = " ( " + selectTagValues + " ) ";
        }

        //拼接子查询
        String selectSubQuerySql = "select groupBitmapMergeState(us) as us from " + selectTableName +
                " where tag_code = '" + selectTagCode + "'  and tag_value " +
                selectOperator + " " + selectTagValues + " and dt = '" + busiDate + "'";

        return selectSubQuerySql;
    }

    /**
     * 确定tag_value后面操作符的方法
     */
    private String getConditionOperator(String operator){
        //通过condition中的operator进行匹配
        switch(operator){
            case "eq":
                return "=";
            case "lte":
                return "<=";
            case "gte":
                return ">=";
            case "lt":
                return "<";
            case "gt":
                return ">";
            case "neq":
                return "<>";
            case "in":
                return "in";
            case "nin":
                return "not in";
        }
        throw new RuntimeException("操作符号不正确！");
    }
}
