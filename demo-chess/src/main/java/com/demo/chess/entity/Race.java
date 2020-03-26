package com.demo.chess.entity;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

import java.io.Serializable;

/**
 * 种族
 */
@Data
public class Race implements Serializable {

    @JSONField(name = "TFTID")
    private String TFTID;
    /** 别名 */
    private String alias;
    /** 图片路径 */
    private String imagePath;
    /** 介绍 */
    private String introduce;
    /** 水平 */
    private String level;
    /** 名字 */
    private String name;
    /** 种族ID */
    private String raceId;

    private String traitId;


}
