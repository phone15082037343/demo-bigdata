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
public class ChessCombine implements Serializable {

    /** 总的价格 */
    private Integer price;
    /** 名字 */
    private List<String> displayNames;
    /** 成功羁绊数 */
    private Map<String, Integer> success;
    /** 未凑成羁绊数 */
    private Map<String, Integer> failed;

}
