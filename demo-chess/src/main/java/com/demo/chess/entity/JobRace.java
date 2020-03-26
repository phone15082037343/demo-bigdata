package com.demo.chess.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

/**
 * job + race
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class JobRace implements Serializable {

    /** 别名 */
    private String alias;
    /** 图片路径 */
    private String imagePath;
    /** 介绍 */
    private String introduce;
    /** id */
    private String id;
    /** 人口数 */
    private Map<Integer, String> level;
    /** 职业名称 */
    private String name;
    /** job 或 race */
    private String type;

}
