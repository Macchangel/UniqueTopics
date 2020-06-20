package com.daslab.czy.SemanticExtractor;

import com.daslab.czy.Utils.MySQLUtils;
import com.daslab.czy.Utils.SparkContextUtils;
import com.daslab.czy.model.Token;
import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary;
import com.hankcs.hanlp.seg.Segment;
import com.hankcs.hanlp.seg.common.Term;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;

import javax.print.Doc;
import java.util.ArrayList;
import java.util.List;

public class Tokenizer {
    public static List<Document> tokenize(List<Document> records){
        System.out.println();
        System.out.println("---------------开始分词--------------");
        long startTime = System.currentTimeMillis();
        List<Document> result = new ArrayList<>();
        for(Document record : records){
            String text = (String) record.get("news_text");
            Segment segment = HanLP.newSegment();
            List<Term> termList = CoreStopWordDictionary.apply(segment.seg(text));
            StringBuffer sb = new StringBuffer();
            for (Term term : termList) {
                sb.append(term.word);
                sb.append(" ");
            }
            record.put("news_words", sb.toString());
            result.add(record);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("切词完成，耗时：" + (endTime - startTime) + "ms");
        return result;
    }

    public static List<Document> tokenizeBySpark(List<Document> records){
        System.out.println();
        System.out.println("---------------开始分词--------------");
        long startTime = System.currentTimeMillis();
        JavaSparkContext sc = SparkContextUtils.getSc();
        JavaRDD<Document> rdd = sc.parallelize(records);
        JavaRDD<Document> rdd1 = rdd.map((Document record) -> {
            String text = (String) record.get("news_text");
            Segment segment = HanLP.newSegment();
            List<Term> termList = CoreStopWordDictionary.apply(segment.seg(text));
            StringBuffer sb = new StringBuffer();
            for (Term term : termList) {
                sb.append(term.word);
                sb.append(" ");
            }
            record.put("news_words", sb.toString());
            return record;
        });
        List<Document> result = rdd1.collect();
        long endTime = System.currentTimeMillis();
        System.out.println("切词完成，耗时：" + (endTime - startTime) + "ms");
        return result;
    }

    public static void main(String[] args) {
        List<Document> records = MySQLUtils.select("news", new String[]{"id", "news_text"});
        records = Tokenizer.tokenize(records);
        MySQLUtils.update(records, "news", "news_words");
    }
}
