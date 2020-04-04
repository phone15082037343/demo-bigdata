package com.demo.chess.module;

import lombok.*;

import java.io.Serializable;
import java.util.List;

/**
 * 棋子
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class Chess implements Serializable {

    /** 棋子ID */
    private Integer chessId;
    /** 展示名，如：安妮 */
    private String displayName;
    /** 黑暗之女 */
    private String title;
    /** 羁绊ID，种族+职业，以","隔开 */
    private List<Integer> jobRaceIds;
    /** 价格 */
    private Integer price;

}
