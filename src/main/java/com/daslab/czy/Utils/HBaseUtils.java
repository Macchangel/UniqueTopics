package com.daslab.czy.Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.bson.Document;

import java.io.IOException;
import java.util.List;

public class HBaseUtils {
    public static void saveToHBase(List<Document> records, String tableName, String colFamily){
        long startTime = System.currentTimeMillis();
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.rootdir","hdfs://localhost:9000/hbase");
        try{
            Connection connection = ConnectionFactory.createConnection(configuration);
            Admin admin = connection.getAdmin();
            createTable(admin, tableName, colFamily);
            Table table = connection.getTable(TableName.valueOf(tableName));
            for(Document record: records){
                String rowKey = record.get("_id").toString();
                Put put = new Put(rowKey.getBytes());
                for(String key: record.keySet()){
                    if(key.equals("_id")){
                        continue;
                    }else{
                        Object value = record.get(key);
                        put.addColumn(colFamily.getBytes(), key.getBytes(), SerializeUtils.serialize(value));
                    }
                }
                table.put(put);
            }
            table.close();
            long endTime = System.currentTimeMillis();
            System.out.println("已存入HBase，耗时：" + (endTime - startTime) + "ms");
        }catch (IOException e){
            e.printStackTrace();
        }
    }


    public static void createTable(Admin admin, String myTableName,String colFamily) throws IOException {
        TableName tableName = TableName.valueOf(myTableName);
        if(admin.tableExists(tableName)){
            System.out.println("Table is exists!");
        }else {
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            tableDescriptor.addFamily(new HColumnDescriptor(colFamily));
            admin.createTable(tableDescriptor);
            System.out.println("Table created");
        }
    }

//    public static List<Document> getFromHBase(String tableName, String colFamily){
//        long startTime = System.currentTimeMillis();
//        Configuration configuration = HBaseConfiguration.create();
//        configuration.set("hbase.rootdir","hdfs://localhost:9000/hbase");
//        try{
//            Connection connection = ConnectionFactory.createConnection(configuration);
//            Table table = connection.getTable(TableName.valueOf(tableName));
//
//            for(Document record: records){
//                String rowKey = record.get("_id").toString();
//                Put put = new Put(rowKey.getBytes());
//                for(String key: record.keySet()){
//                    if(key.equals("_id")){
//                        continue;
//                    }else{
//                        Object value = record.get(key);
//                        put.addColumn(colFamily.getBytes(),key.getBytes(), SerializeUtils.serialize(value));
//                    }
//                }
//                table.put(put);
//            }
//            table.close();
//            long endTime = System.currentTimeMillis();
//            System.out.println("从HBase取出数据，耗时：" + (endTime - startTime) + "ms");
//        }catch (IOException e){
//            e.printStackTrace();
//        }
//        return null;
//    }
}
