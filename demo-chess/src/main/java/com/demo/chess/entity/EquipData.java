package com.demo.chess.entity;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class EquipData implements Serializable {

    private String season;
    private String time;
    private String version;
    private List<Equip> data;


}