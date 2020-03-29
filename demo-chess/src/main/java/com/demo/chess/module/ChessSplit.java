package com.demo.chess.module;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class ChessSplit implements Serializable, Cloneable {

    /** 棋子ID */
    private Integer chessId;
    /** 展示名，如：安妮 */
    private String displayName;
    /** 黑暗之女 */
    private String title;
    /** 价格 */
    private Integer price;
    /** 羁绊ID */
    private Integer jobRaceId;

    @Override
    public ChessSplit clone() throws CloneNotSupportedException {
        return (ChessSplit) super.clone();
    }
}
