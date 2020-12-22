package com.daslab.czy.Utils;

import com.daslab.czy.SemanticExtractor.Tokenizer;
import com.daslab.czy.model.Token;
import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MySQLUtils {
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://10.131.238.35:3306/Chinanews?characterEncoding=utf-8";
    static final String USER = "root";
    static final String PASS = "123456";



    public static void insert(List<Document> records, String tableName) {

        System.out.println("-------------开始插入数据到MySQL---------------");
        long startTime = System.currentTimeMillis();
        Connection conn = null;
        Statement stmt = null;
        int successCount = 0;
        int failCount = 0;
        int errorCount = 0;
        int count = 0;
        System.out.println(records.size());
        try{
            // 注册 JDBC 驱动
            Class.forName(JDBC_DRIVER);

            // 打开链接
            conn = DriverManager.getConnection(DB_URL,USER,PASS);

            conn.setAutoCommit(false);

            // 执行查询
            stmt = conn.createStatement();
            String sql = "INSERT INTO " + tableName + "(news_title, news_url, news_date, news_location, news_child_location, news_text) VALUES ('%s', '%s', '%s', '%s', '%s', '%s')";
            System.out.println(records.size());
            for(Document record : records){
                count++;

                String news_tile = (String) record.get("news_title");
                String news_url = (String) record.get("news_url");
                String news_date = (String) record.get("news_date");
                String news_location = (String) record.get("news_location");
                String news_child_location = (String) record.get("news_child_location");
                String news_text = (String) record.get("news_text");
                String s = String.format(sql, news_tile, news_url, news_date, news_location, news_child_location, news_text);
//                System.out.println(s);
                try{
                    if(stmt.executeUpdate(s) != 0){
//                    System.out.println("插入成功");
                        successCount++;
                    }else{
                        System.out.println("插入失败");
                        failCount++;
                    }
                }catch (MySQLSyntaxErrorException e){
                    System.out.println("MySQLSynError：" + s);
                    errorCount++;
                }catch (SQLException e){
                    System.out.println("SQLException:" + s);
                    errorCount++;
                }
            }
            conn.commit();

            stmt.close();
            conn.close();
        }catch(SQLException se){
            // 处理 JDBC 错误
            se.printStackTrace();
        }catch(Exception e){
            // 处理 Class.forName 错误
            e.printStackTrace();
        }finally{
            // 关闭资源
            try{
                if(stmt!=null) stmt.close();
            }catch(SQLException se2){
            }// 什么都不做
            try{
                if(conn!=null) conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }
        }
        long endTime = System.currentTimeMillis();
        System.out.println("数据插入MySQL完成，共插入" + successCount + "条记录，耗时：" + (endTime - startTime) + "ms");
        System.out.println(successCount + " " + failCount + " " + errorCount + " " + count);
    }


    public static void update(List<Document> records, String tableName, String columnName) {

        System.out.println("-------------开始更新数据到MySQL---------------");
        long startTime = System.currentTimeMillis();
        Connection conn = null;
        Statement stmt = null;
        int successCount = 0;
        int failCount = 0;
        int errorCount = 0;
        int count = 0;
        System.out.println(records.size());
        try{
            // 注册 JDBC 驱动
            Class.forName(JDBC_DRIVER);

            // 打开链接
            conn = DriverManager.getConnection(DB_URL,USER,PASS);

            conn.setAutoCommit(false);

            // 执行查询
            stmt = conn.createStatement();

            String sql = "UPDATE " + tableName + " SET " + columnName +"='%s' WHERE id=%s";
            for(Document record : records){
                count++;
                String id = record.getString("id");
                String columnValue = (String) record.get(columnName);
                String s = String.format(sql, columnValue, id);
//                System.out.println(s);
                try{
                    if(stmt.executeUpdate(s) != 0){
//                    System.out.println("插入成功");
                        successCount++;
                    }else{
                        System.out.println("插入失败");
                        failCount++;
                    }
                }catch (MySQLSyntaxErrorException e){
                    System.out.println("MySQLSynError：" + s);
                    e.printStackTrace();
                    errorCount++;
                }catch (SQLException e){
                    System.out.println("SQLException:" + s);
                    errorCount++;
                }
            }
            conn.commit();

            stmt.close();
            conn.close();
        }catch(SQLException se){
            // 处理 JDBC 错误
            se.printStackTrace();
        }catch(Exception e){
            // 处理 Class.forName 错误
            e.printStackTrace();
        }finally{
            // 关闭资源
            try{
                if(stmt!=null) stmt.close();
            }catch(SQLException se2){
            }// 什么都不做
            try{
                if(conn!=null) conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }
        }
        long endTime = System.currentTimeMillis();
        System.out.println("数据更新MySQL完成，共更新" + successCount + "条记录，耗时：" + (endTime - startTime) + "ms");
        System.out.println(successCount + " " + failCount + " " + errorCount + " " + count);
    }

    public static List<Document> select(String tableName, String[] columnNames) {

        System.out.println("-------------开始查找MySQL---------------");
        long startTime = System.currentTimeMillis();
        Connection conn = null;
        Statement stmt = null;

        List<Document> records = new ArrayList<>();

        try{
            // 注册 JDBC 驱动
            Class.forName(JDBC_DRIVER);

            // 打开链接
            conn = DriverManager.getConnection(DB_URL,USER,PASS);


            // 执行查询
            stmt = conn.createStatement();
            String cols = StringUtils.join(columnNames, ",");
            String sql = "SELECT " + cols + " FROM " + tableName;

            ResultSet resultSet = stmt.executeQuery(sql);
            while (resultSet.next()){
                Document record = new Document();
                for(String columnName : columnNames){
                    record.put(columnName, resultSet.getString(columnName));
                }
                records.add(record);
            }

            stmt.close();
            conn.close();
        }catch(SQLException se){
            // 处理 JDBC 错误
            se.printStackTrace();
        }catch(Exception e){
            // 处理 Class.forName 错误
            e.printStackTrace();
        }finally{
            // 关闭资源
            try{
                if(stmt!=null) stmt.close();
            }catch(SQLException se2){
            }// 什么都不做
            try{
                if(conn!=null) conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }
        }
        long endTime = System.currentTimeMillis();
        System.out.println("MySQL数据查找完成，共找到" + records.size() + "条记录，耗时：" + (endTime - startTime) + "ms");
        return records;
    }


    public static List<String> getWords(String sql) {

        Connection conn = null;
        Statement stmt = null;

        List<String> result = new ArrayList<>();

        try{
            // 注册 JDBC 驱动
            Class.forName(JDBC_DRIVER);

            // 打开链接
            conn = DriverManager.getConnection(DB_URL,USER,PASS);


            // 执行查询
            stmt = conn.createStatement();


            ResultSet resultSet = stmt.executeQuery(sql);
            while (resultSet.next()){
                result.add(resultSet.getString("news_words"));
            }

            stmt.close();
            conn.close();
        }catch(SQLException se){
            // 处理 JDBC 错误
            se.printStackTrace();
        }catch(Exception e){
            // 处理 Class.forName 错误
            e.printStackTrace();
        }finally{
            // 关闭资源
            try{
                if(stmt!=null) stmt.close();
            }catch(SQLException se2){
            }// 什么都不做
            try{
                if(conn!=null) conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }
        }
        return result;
    }

    public static Map<Integer, String> getIdsAndRawDocs(String sql) {

        Connection conn = null;
        Statement stmt = null;

        Map<Integer, String> result = new HashMap<>();

        try{
            // 注册 JDBC 驱动
            Class.forName(JDBC_DRIVER);

            // 打开链接
            conn = DriverManager.getConnection(DB_URL,USER,PASS);


            // 执行查询
            stmt = conn.createStatement();


            ResultSet resultSet = stmt.executeQuery(sql);
            while (resultSet.next()){
                result.put(resultSet.getInt("id"), resultSet.getString("news_words"));
            }

            stmt.close();
            conn.close();
        }catch(SQLException se){
            // 处理 JDBC 错误
            se.printStackTrace();
        }catch(Exception e){
            // 处理 Class.forName 错误
            e.printStackTrace();
        }finally{
            // 关闭资源
            try{
                if(stmt!=null) stmt.close();
            }catch(SQLException se2){
            }// 什么都不做
            try{
                if(conn!=null) conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }
        }
        return result;
    }










    public static void main(String[] args) {
        List<Document> records = MySQLUtils.select("news_test", new String[]{"id", "news_text"});
        System.out.println(records.size());
        List<Document> little_records = new ArrayList<>();
        for(int i = 0; i < records.size(); i++){
            little_records.add(records.get(i));
        }
        Tokenizer.tokenize(little_records);

        Tokenizer.tokenizeBySpark(little_records);
//        MySQLUtils.update(little_records, "news_test", "news_words");
    }
}
