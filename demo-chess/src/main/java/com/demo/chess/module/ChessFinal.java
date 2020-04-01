package com.demo.chess.module;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * 棋子
 */
@Data
@Builder
public class ChessFinal implements Serializable {

    /** 棋子ID */
    private Integer chessId;
    /** 展示名，如：安妮 */
    private String displayName;
    /** 黑暗之女 */
    private String title;
    /** 价格 */
    private Integer price;
    /** key:羁绊名称,value：凑成数集合 */
    private Map<String, List<Integer>> level;

}
