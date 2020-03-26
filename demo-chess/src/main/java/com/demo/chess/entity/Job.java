package com.demo.chess.entity;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;

/**
 * 职业
 */
@Data
@Builder
public class Job implements Serializable {

    @JSONField(name = "TFTID")
    private String TFTID;
    /** 别名 */
    private String alias;
    /** 图片路径 */
    private String imagePath;
    /** 介绍 */
    private String introduce;
    /** 职业ID */
    private String jobId;
    /** 水平 */
    private Map<Integer, String> level;
    /** 职业名称 */
    private String name;

    private String traitId;


}
