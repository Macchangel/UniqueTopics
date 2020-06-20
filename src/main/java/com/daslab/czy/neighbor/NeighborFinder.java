package com.daslab.czy.neighbor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.FileUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NeighborFinder {
    List<MapUnit> mapUnitList = new ArrayList<>();

    public void parseJsonList(String inputDirectory){
        File file = new File(inputDirectory);
        String[] fileList = file.list();
        for(String fileName : fileList){
            String inputFileName = inputDirectory + fileName;
            parseJson(inputFileName);
        }
    }

    public void parseJson(String inputFileName){
        File file = new File(inputFileName);
        String file1 = null;
        try {
            file1 = FileUtils.readFileToString(file);
        } catch (IOException e) {
            e.printStackTrace();
        }

        JSONObject jsonObject = JSON.parseObject(file1);
        JSONArray features = jsonObject.getJSONArray("features");
        for(int i = 0; i < features.size(); i++){
            JSONObject feature = features.getJSONObject(i);
            JSONObject properties = feature.getJSONObject("properties");
            String id = properties.getString("id");
            String name = properties.getString("name");
            JSONObject geometry = feature.getJSONObject("geometry");
            JSONArray coordinates = geometry.getJSONArray("coordinates").getJSONArray(0);
            String type = geometry.getString("type");

            List<double[]> cds = new ArrayList<>();
            double maxX = -1000000.0;
            double maxY = -1000000.0;
            double minX = 1000000.0;
            double minY = 1000000.0;

            if(type.equals("Polygon")){
                // 因为第一个点会出现两次，所以从下标为1的点开始遍历
                for(int j = 1; j < coordinates.size(); j++){
                    JSONArray coordinate = coordinates.getJSONArray(j);
                    double x = coordinate.getDouble(0);
                    double y = coordinate.getDouble(1);
                    if(x > maxX){
                        maxX = x;
                    }else if(x < minX){
                        minX = x;
                    }
                    if(y > maxY){
                        maxY = y;
                    }else if(y < minY){
                        minY = y;
                    }
                    cds.add(new double[]{x, y});
                }
            }else if(type.equals("MultiPolygon")){
                for(int k = 0; k < coordinates.size(); k++){
                    JSONArray real_coordinates = coordinates.getJSONArray(k);
                    // 因为第一个点会出现两次，所以从下标为1的点开始遍历
                    for(int j = 1; j < real_coordinates.size(); j++){
                        JSONArray coordinate = real_coordinates.getJSONArray(j);
                        double x = coordinate.getDouble(0);
                        double y = coordinate.getDouble(1);
                        if(x > maxX){
                            maxX = x;
                        }else if(x < minX){
                            minX = x;
                        }
                        if(y > maxY){
                            maxY = y;
                        }else if(y < minY){
                            minY = y;
                        }
                        cds.add(new double[]{x, y});
                    }
                }
            }

            MapUnit mapUnit = new MapUnit(id, name, cds, maxX, maxY, minX, minY);
            mapUnitList.add(mapUnit);
        }
    }

    public void findNeighbor(){
        for(int i = 0; i < mapUnitList.size(); i++){
            MapUnit curr = mapUnitList.get(i);
            for(int j = i + 1; j < mapUnitList.size(); j++){
                MapUnit other = mapUnitList.get(j);
                if(curr.minX > other.maxX || curr.minY > other.maxY || curr.maxX < other.minX || curr.maxY < other.minY){
                    continue;
                }
                boolean hasFound = false;
                for(double[] currCd : curr.coordinates){
                    for(double[] otherCd : other.coordinates){
                        if(currCd[0] == otherCd[0] && currCd[1] == otherCd[1]){
                            curr.neighbors.add(other.id);
                            other.neighbors.add(curr.id);
                            System.out.println(curr.id + " " + other.id + " 是邻居。");
                            hasFound = true;
                            break;
                        }
                    }
                    if (hasFound){
                        break;
                    }
                }
            }
        }
    }

    void save(String outputFileName) throws IOException {
        File file = new File(outputFileName);
        if(!file.exists()){
            file.createNewFile();
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));

        for(MapUnit mapUnit : mapUnitList){
            writer.write(mapUnit.id + ":");
            for(String neighbor : mapUnit.neighbors){
                if(neighbor.substring(0, 2).equals(mapUnit.id.substring(0, 2))){
                    writer.write(neighbor + ",");
                }
            }
            writer.write("\n");
        }
        writer.close();
    }

    public static void main(String[] args) throws IOException {
        NeighborFinder neighborFinder = new NeighborFinder();
//        neighborFinder.parseJson("/home/leaves1233/IdeaProjects/UniqueTopics/src/main/resources/map/china.json");
//        neighborFinder.findNeighbor();
//        neighborFinder.save("/home/leaves1233/IdeaProjects/UniqueTopics/src/main/resources/map/chinaNeighbor.txt");
        neighborFinder.parseJsonList("/home/leaves1233/IdeaProjects/UniqueTopics/src/main/resources/map/geometryProvince/");
        neighborFinder.findNeighbor();
        neighborFinder.save("/home/leaves1233/IdeaProjects/UniqueTopics/src/main/resources/map/neighborWithoutOtherPro.txt");
    }
}
