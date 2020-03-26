package com.demo.chess.entity;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

import java.io.Serializable;

/**
 * 棋子
 */
@Data
public class Chess implements Serializable, Cloneable {

    @JSONField(name = "TFTID")
    private String TFTID;
    /** 护甲 */
    private String armor;
    /** 攻击力 */
    private String attack;
    /** 攻击力成长数据，以"/"隔开 */
    private String attackData;

    private String attackMag;
    /** 攻击范围 */
    private String attackRange;
    /** 攻速 */
    private String attackSpeed;

    private String chessId;
    /** 暴击率 */
    private String crit;
    /** 名字 */
    private String displayName;
    /** 说明 */
    private String illustrate;

    private String jobIds;
    /** 职业 */
    private String jobs;
    /** 生命值 */
    private String life;
    /** 生命值成长，以"/"隔开 */
    private String lifeData;

    private String lifeMag;
    /** 法力值 */
    private String magic;
    /** 图片名字 */
    private String name;
    /** 原始图片路径 */
    private String originalImage;
    /** 价格 */
    private String price;
    /** 状态 */
    private String proStatus;
    /** 种族IDs */
    private String raceIds;
    /** 种族 */
    private String races;
    /** 装备推荐IDs */
    private String recEquip;
    /** 技能详情 */
    private String skillDetail;
    /** 技能图片 */
    private String skillImage;
    /** 技能介绍 */
    private String skillIntroduce;
    /** 技能名称 */
    private String skillName;
    /** 技能类型 */
    private String skillType;
    /** 魔抗 */
    private String spellBlock;
    /** 初始法力值 */
    private String startMagic;

    private String synergies;
    /** 标题 */
    private String title;

    /** 职业ID */
    private String jobId;
    /** 种族ID */
    private String raceId;

    @Override
    public Chess clone() throws CloneNotSupportedException {
        return (Chess) super.clone();
    }
}
