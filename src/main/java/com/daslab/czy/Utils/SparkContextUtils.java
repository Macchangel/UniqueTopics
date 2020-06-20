package com.daslab.czy.Utils;

import com.daslab.czy.model.Token;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;

public class SparkContextUtils {
    private volatile static JavaSparkContext sc;
    private SparkContextUtils(){};
    public static JavaSparkContext getSc(){
        if(sc == null){
            synchronized (SparkContextUtils.class){
                if(sc == null){
                    SparkConf sparkConf = new SparkConf();
                    sparkConf.set("spark.network.timeout", "600s");
                    sparkConf.set("spark.driver.memory", "20G");
                    sparkConf.set("spark.executor.memory", "20G");
                    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
                    sparkConf.set("spark.kryoserializer.buffer.max", "512m");
                    sparkConf.set("spark.kryoserializer.buffer", "64m");
                    sparkConf.registerKryoClasses(new Class[]{Document.class, Token.class});
                    sc = new JavaSparkContext("local[6]", "BiShe", sparkConf);
                }
            }
        }
        return sc;
    }
}

