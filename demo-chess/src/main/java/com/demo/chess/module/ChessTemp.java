package com.demo.chess.module;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * 组合之后的结果
 */
@Data
@Builder
@Deprecated
public class ChessTemp implements Serializable {

    private String uuid;
    private Integer chessId;
    private String displayName;
    private Integer jobRaceId;
    private Integer price;
    private String title;
    private String name;
    private List<Integer> level;

}
