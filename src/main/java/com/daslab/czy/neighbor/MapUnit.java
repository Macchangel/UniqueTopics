package com.daslab.czy.neighbor;

import java.util.ArrayList;
import java.util.List;

public class MapUnit {
    String id;
    String name;
    List<double[]> coordinates;
    double maxX;
    double maxY;
    double minX;
    double minY;
    List<String> neighbors;

    public MapUnit(String id, String name, List<double[]> coordinates, double maxX, double maxY, double minX, double minY){
        this.id = id;
        this.name = name;
        this.coordinates = coordinates;
        this.maxX = maxX;
        this.maxY = maxY;
        this.minX = minX;
        this.minY = minY;
        this.neighbors = new ArrayList<>();
    }
}
