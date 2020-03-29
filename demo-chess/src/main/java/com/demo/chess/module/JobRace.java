package com.demo.chess.module;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
@Builder
public class JobRace implements Serializable {

    /** ID */
    private Integer jobRaceId;
    /** 名字 */
    private String name;
    /** key:人口数，value:羁绊描述 */
    private List<Integer> level;

}
