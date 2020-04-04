package com.demo.chess.module;

import lombok.*;

import java.io.Serializable;

@Data
@Builder
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class ChessCombineOutput implements Serializable {

    /** 棋子集合 */
    private String displayName;
    /** 成功羁绊 */
    private String success;
    /** 成功羁绊数 */
    private Integer successNum;
    /** 总的价格 */
    private Integer price;
    /** 未凑成羁绊 */
    private String faield;
    /** 未凑成羁绊数 */
    private Integer faieldNum;

}
