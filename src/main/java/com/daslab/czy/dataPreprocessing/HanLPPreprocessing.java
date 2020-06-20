package com.daslab.czy.dataPreprocessing;


import com.daslab.czy.Utils.SparkContextUtils;
import com.daslab.czy.model.Token;
import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.Segment;
import com.hankcs.hanlp.seg.common.Term;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;

import java.util.*;

import static com.daslab.czy.Utils.HBaseUtils.saveToHBase;
import static com.daslab.czy.Utils.MongoDBUtils.getDataFromMongoDB;


public class HanLPPreprocessing {


    public static List<Document> preprocessing(List<Document> records){
        System.out.println();
        System.out.println("---------------开始预处理--------------");
        long startTime = System.currentTimeMillis();
        JavaSparkContext sc = SparkContextUtils.getSc();
        JavaRDD<Document> rdd = sc.parallelize(records);
        JavaRDD<Document> rdd1 = rdd.map((Document d) -> {
            String text = (String) d.get("news_text");
            Segment segment = HanLP.newSegment().enableIndexMode(true);
            List<Term> termList = segment.seg(text);
            List<Token> tokens = new ArrayList<>();
            for (Term term : termList) {
                tokens.add(new Token(term.word, term.nature.toString(), term.offset, (term.offset + term.word.length())));
            }
            d.put("tokens", tokens);
            return d;
        });
        List<Document> result = rdd1.collect();
        long endTime = System.currentTimeMillis();
        System.out.println("预处理完成，耗时：" + (endTime - startTime) + "ms");
        return result;
    }



    public static void main(String[] args) {
        List<Document> records= preprocessing(getDataFromMongoDB("localhost", "Chinanews", new String[]{"Sh"}));
        System.out.println(records);
        saveToHBase(records, "Sh", "colFamily1");
    }




}
