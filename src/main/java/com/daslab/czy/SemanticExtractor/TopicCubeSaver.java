package com.daslab.czy.SemanticExtractor;

import antlr.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class TopicCubeSaver {

    String hBaseLoc;
    String hBaseUrl;
    String tableName;
    String colFamily;

    public TopicCubeSaver(String hBaseLoc, String hBaseUrl, String tableName) throws IOException {
        this.hBaseLoc = hBaseLoc;
        this.hBaseUrl = hBaseUrl;
        this.tableName = tableName;

        Configuration configuration = HBaseConfiguration.create();
        configuration.set(hBaseLoc, hBaseUrl);
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf(tableName));
    }

    public void saveProToHbase(String dirPath, String prefix, int K) throws IOException {
        long startTime = System.currentTimeMillis();
        System.out.println();
        System.out.println("存入Hbase");

        Configuration configuration = HBaseConfiguration.create();
        configuration.set(hBaseLoc, hBaseUrl);
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf(tableName));

        File dir = new File(dirPath);
        String[] fileNames = dir.list();
        for(String fileName : fileNames){
            String filePath = dirPath + '/' + fileName;
            float[] topicPro = readDatasetTopicPro(filePath, K);
            String rowKey = prefix + fileName.split("D")[0];
            System.out.println(rowKey);
            Put put = new Put(rowKey.getBytes());
            for(int i = 0; i < K; i++){
                String key = "topic" + i;
                String value = String.valueOf(topicPro[i]);
                put.addColumn(colFamily.getBytes(), key.getBytes(), value.getBytes());
            }
            table.put(put);
        }

        table.close();

        long endTime = System.currentTimeMillis();
        System.out.println("已存入HBase，耗时：" + (endTime - startTime) + "ms");

    }

    public float[] readDatasetTopicPro(String filePath, int K) throws IOException{
        float[] topicPro=new float[K];
        InputStreamReader inStrR = new InputStreamReader(new FileInputStream(filePath));
        BufferedReader br = new BufferedReader(inStrR);
        String line = br.readLine();
        String[] topicCount=line.split(" ");
        for(int i = 0; i < K; i++){
            topicPro[i] = Float.parseFloat(topicCount[i*2+1]);
        }
        br.close();
        return topicPro;
    }

    static float get(String id, int topic, String tableName, String colFamily) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","10.131.238.35");
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf(tableName));

        Get get = new Get(id.getBytes());
        String col = "topic" + topic;
        get.addColumn(colFamily.getBytes(), col.getBytes());
        Result result = table.get(get);
        table.close();
        return Float.parseFloat(new String(result.getValue(colFamily.getBytes(), col.getBytes())));
    }

    public void saveMToHbase(String filePath, String prefix) throws IOException {
        long startTime = System.currentTimeMillis();
        System.out.println();
        System.out.println("存入Hbase");

        Configuration configuration = HBaseConfiguration.create();
        configuration.set(hBaseLoc, hBaseUrl);
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf(tableName));

        InputStreamReader inStrR = new InputStreamReader(new FileInputStream(filePath));
        BufferedReader br = new BufferedReader(inStrR);
        String line = br.readLine();
        while (line!= null){
            String M = line.split(":")[1];
            String rowKey = prefix + line.split(":")[0];
            Put put = new Put(rowKey.getBytes());
            String col = "M";
            put.addColumn(colFamily.getBytes(), col.getBytes(), M.getBytes());
            table.put(put);

            line = br.readLine();
        }
        table.close();

        long endTime = System.currentTimeMillis();
        System.out.println("已存入HBase，耗时：" + (endTime - startTime) + "ms");

    }

    public void saveMToHbase2(String filePath, String prefix) throws IOException, ParseException {
        String startDate = "2019-01-01";
        String endDate = "2020-05-31";


        long startTime = System.currentTimeMillis();
        System.out.println();
        System.out.println("存入Hbase");

        Configuration configuration = HBaseConfiguration.create();
        configuration.set(hBaseLoc, hBaseUrl);
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf(tableName));

        InputStreamReader inStrR = new InputStreamReader(new FileInputStream(filePath));
        BufferedReader br = new BufferedReader(inStrR);
        String line = br.readLine();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date startDt = sdf.parse(startDate);
        Date endDt = sdf.parse(endDate);
        Calendar rightNow = Calendar.getInstance();
        rightNow.setTime(startDt);
        String date = sdf.format(rightNow.getTime());

        boolean flag = false;
        int count = 0;

        while (line!= null){
            String M = line.split(":")[1];
            String name = line.split(":")[0];
            if(name.length() == 2){
                if(name.equals("44")){
                    if(!flag){
                        flag = true;
                    }else {
                        rightNow.add(Calendar.MONTH, 1);
                        date = sdf.format(rightNow.getTime());
                    }
                }
                String rowKey = prefix + date + name;
                Put put = new Put(rowKey.getBytes());
                String col = "M";
                put.addColumn(colFamily.getBytes(), col.getBytes(), M.getBytes());
                table.put(put);
            }


            line = br.readLine();
        }
        table.close();

        long endTime = System.currentTimeMillis();
        System.out.println("已存入HBase，耗时：" + (endTime - startTime) + "ms");

    }

    private void saveMToHBase3(String filePath, String prefix) throws IOException, ParseException {
        String startDate = "2019-01-01";
        String endDate = "2020-05-31";


        long startTime = System.currentTimeMillis();
        System.out.println();
        System.out.println("存入Hbase");

        Configuration configuration = HBaseConfiguration.create();
        configuration.set(hBaseLoc, hBaseUrl);
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf(tableName));

        InputStreamReader inStrR = new InputStreamReader(new FileInputStream(filePath));
        BufferedReader br = new BufferedReader(inStrR);
        String line = br.readLine();
        line = br.readLine(); //跳过第一行

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date startDt = sdf.parse(startDate);
        Date endDt = sdf.parse(endDate);
        Calendar rightNow = Calendar.getInstance();
        rightNow.setTime(startDt);
        String date = sdf.format(rightNow.getTime());

        boolean flag = false;
        int count = 0;

        while (line!= null){
            String M = line.split(":")[1];
            String name = line.split(":")[0];
            if(name.length() == 2){
                if(name.equals("44")){
                    if(!flag){
                        flag = true;
                    }else {
                        rightNow.add(Calendar.MONTH, 1);
                        date = sdf.format(rightNow.getTime());
                    }
                }
            }

            if(name.length() > 2){
                String rowKey = prefix + date + name;
                System.out.println(rowKey);
                Put put = new Put(rowKey.getBytes());
                String col = "M";
                put.addColumn(colFamily.getBytes(), col.getBytes(), M.getBytes());
                table.put(put);
            }


            line = br.readLine();
        }
        table.close();

        long endTime = System.currentTimeMillis();
        System.out.println("已存入HBase，耗时：" + (endTime - startTime) + "ms");
    }

    public void saveToHBase(String rowKey, List<Double> topicDistribution, List<List<Integer>> postingList, int M){
        Put put = new Put(rowKey.getBytes());

        //save topicDistribution
        for(int i = 0; i < topicDistribution.size(); i++){
            String col = "topic" + i;
            String value = String.valueOf(topicDistribution.get(i));
            put.addColumn("Topic Distribution".getBytes(), col.getBytes(), value.getBytes());
        }

        //save postingList
        for(int i = 0; i < postingList.size(); i++){
            String col = "topic" + i;
            List<Integer> list = postingList.get(i);
            String value = "";
            for(int j = 0; j < list.size(); j++){
                if(j != 0){
                    value += ",";
                }
                value += list.get(j);
            }
            put.addColumn("Posting List".getBytes(), col.getBytes(), value.getBytes());
        }

        //save M
        put.addColumn("Other Params".getBytes(), "M".getBytes(), String.valueOf(M).getBytes());
    }


    public static void main(String[] args) throws IOException, ParseException {
//        TopicCubeSaver tps = new TopicCubeSaver("hbase.rootdir", "hdfs://localhost:9000/hbase", "news_test", "topic");

//        tps.saveToHbase(timePath, timePrefix, K);
//        tps.saveToHbase(provincePath, provincePrefix, K);
//        tps.saveProToHbase(cityPath, cityPrefix, K);
//        tps.saveMToHbase("/home/leaves1233/IdeaProjects/UniqueTopics/temp/data/writePath2/M.txt", "100");
//        tps.saveMToHbase2("/home/leaves1233/IdeaProjects/UniqueTopics/temp/data/M.txt", "110");
//        tps.saveMToHBase3("/home/leaves1233/IdeaProjects/UniqueTopics/temp/data/M.txt", "111");
    }




}
