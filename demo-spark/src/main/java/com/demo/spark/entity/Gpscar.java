package com.demo.spark.entity;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.sql.Timestamp;

@Data
@Builder
public class Gpscar implements Serializable {

    private String carnumber;
    private Double direction;
    private Double speed;
    private Timestamp gpstime;
    private Double longitude;
    private Double latitude;
    private Integer carstate;

}
