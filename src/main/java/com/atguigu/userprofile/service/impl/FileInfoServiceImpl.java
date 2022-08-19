package com.atguigu.userprofile.service.impl;

import com.atguigu.userprofile.bean.FileInfo;
import com.atguigu.userprofile.mapper.FileInfoMapper;
import com.atguigu.userprofile.service.FileInfoService;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

@Service
@DS("mysql")
public class FileInfoServiceImpl extends ServiceImpl<FileInfoMapper, FileInfo> implements FileInfoService {

}
