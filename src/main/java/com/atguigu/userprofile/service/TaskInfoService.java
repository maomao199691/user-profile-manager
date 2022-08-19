package com.atguigu.userprofile.service;

import com.atguigu.userprofile.bean.TagInfo;
import com.atguigu.userprofile.bean.TaskInfo;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;


public interface TaskInfoService extends IService<TaskInfo> {
    public void saveTaskInfoWithTag( TaskInfo taskInfo);

    public TaskInfo getTaskInfoWithTag(Long taskId);




}
