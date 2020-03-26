package com.demo.chess.entity;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

import java.io.Serializable;

/**
 * 装备
 */
@Data
public class Equip implements Serializable {

    @JSONField(name = "TFTID")
    private String TFTID;
    /** 效果 */
    private String effect;
    private String equipId;
    private String formula;
    private String imagePath;
    private String jobId;
    private String keywords;
    /** 装备名称 */
    private String name;
    private String proStatus;
    private String raceId;
    private String type;

}
