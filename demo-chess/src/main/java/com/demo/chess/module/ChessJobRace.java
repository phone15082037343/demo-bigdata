package com.demo.chess.module;

import lombok.*;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
@Builder
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class ChessJobRace implements Serializable {

    /** 棋子ID */
    private Integer chessId;
    /** 展示名，如：安妮 */
    private String displayName;
    /** 黑暗之女 */
    private String title;
    /** 羁绊ID */
    private Integer jobRaceId;
    /** 价格 */
    private Integer price;
    /** 名字 */
    private String name;
    /** 羁绊数 */
    private List<Integer> level;

}
