package com.demo.chess.module;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
@Builder
@Deprecated
public class ChessJobRace implements Serializable {

    /** 棋子ID */
    private Integer chessId;
    /** 展示名，如：安妮 */
    private String displayName;
    /** 黑暗之女 */
    private String title;
    /** 羁绊ID，种族+职业，以","隔开 */
    private String jobRaceIds;
    /** 价格 */
    private Integer price;
    /** ID */
    private Integer jobRaceId;
    /** 名字 */
    private String name;
    /** 羁绊树 */
    private List<Integer> level;

}
