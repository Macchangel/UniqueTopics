package com.daslab.czy.dataPreprocessing;

import com.daslab.czy.Utils.MongoDBUtils;
import org.bson.Document;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class HanLPPreprocessingTest {
    static List<Document> records;

    @BeforeClass
    public static void setUpBeforeClass(){
        records = MongoDBUtils.getDataFromMongoDB("localhost", "Chinanews", new String[]{"201901"});
    }

    @Test
    public void preprocessing() {
        List<Document> result = HanLPPreprocessing.preprocessing(records);
        System.out.println(result.get(0));
    }
}
