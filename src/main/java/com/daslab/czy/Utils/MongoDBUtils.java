package com.daslab.czy.Utils;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class MongoDBUtils {
    public static List<Document> getDataFromMongoDB(String mongoUri, String mongoDB, String[] collection_list){
        System.out.println();
        System.out.println("-------------开始从Mongo获取数据-----------------");
        long startTime = System.currentTimeMillis();
        MongoClient mongoClient = new MongoClient( mongoUri , 27017 );
        MongoDatabase mongoDatabase = mongoClient.getDatabase(mongoDB);
        System.out.println("Connect to database successfully");

        List<Document> result = new ArrayList<>();
        for(String c : collection_list) {
            MongoCollection<Document> collection = mongoDatabase.getCollection(c);
            System.out.println("Collection:" + c + "选择成功");
            FindIterable<Document> findIterable = collection.find();
            MongoCursor<Document> mongoCursor = findIterable.iterator();
            while (mongoCursor.hasNext()) {
                Document next = mongoCursor.next();
                result.add(next);
            }
        }
        long endTime = System.currentTimeMillis();
        System.out.println("已从MongoDB取出数据，耗时：" + (endTime - startTime) + "ms");
        return result;
    }
}
