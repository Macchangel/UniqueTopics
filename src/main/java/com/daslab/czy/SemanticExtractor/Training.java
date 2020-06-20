package com.daslab.czy.SemanticExtractor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.daslab.czy.Utils.MySQLUtils;
import com.daslab.czy.Utils.SparkContextUtils;
import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.LDAModel;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.bson.Document;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Training {
    private List<String> vocab;
    private int vocabSize;
    private String startDate;
    private String endDate;
    private int globalK;
    private int iterNum;
    private int topK;
    private String writePath;
    private int localK;
    private String tableName;

    private float globalAlpha;
    private float globalBeta;
    private float localAlpha;
    private float localBeta;
    private int W;

    private Map<String, String> provinceDictionary;


    public Training(List<String> vocab, int vocabSize, String startDate, String endDate, int globalK, int iterNum, int topK, String writePath, int localK, String tableName){
        this.vocabSize = vocabSize;
        this.startDate = startDate;
        this.endDate = endDate;
        this.globalK = globalK;
        this.iterNum = iterNum;
        this.topK = topK;
        this.writePath = writePath;
        this.localK = localK;
        this.tableName = tableName;

        this.globalAlpha = (float)1.0 / globalK;
        this.globalBeta = (float)1.0 / vocabSize;
        this.localAlpha = (float)1.0 / localK;
        this.localBeta = (float)1.0 / vocabSize;

        this.vocab = vocab.subList(0, Math.min(vocabSize, vocab.size()));
        this.W = this.vocab.size();
    }

    private void initProvinceDictionary() {
        provinceDictionary = new HashMap<>();

        String dictionaryName = writePath + "provinces.json";
        File file = new File(dictionaryName);
        String file1 = null;
        try {
            file1 = FileUtils.readFileToString(file);
        } catch (IOException e) {
            e.printStackTrace();
        }

        JSONArray jsonArray = JSON.parseArray(file1);
        for(int i = 0; i < jsonArray.size(); i++){
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            String code = jsonObject.getString("code");
            String name = jsonObject.getString("name");
            provinceDictionary.put(code, name);
        }
    }

    public void train() throws IOException, ParseException {
//        initProvinceDictionary();

        System.out.println();
        System.out.println("-------------开始训练全局LDA---------------");
        long startTime = System.currentTimeMillis();

        // load the data
        ReadParseDocs readParse = new ReadParseDocs(W, vocab, tableName);
        readParse.readDocs(startDate, endDate);
        readParse.parseDocs();
        int M = readParse.getM();
        int[][] docs = readParse.getDocs();
        int[] docLength = readParse.getDocLength();

        LDA lda = new LDA(globalK, iterNum, globalAlpha, globalBeta, docs, docLength, W, M, vocab);
        lda.initial();
        lda.gibbsSampling();

        lda.sortWriteTopicPro(topK,writePath+"globalSortedTopicPro.txt");
        lda.writeTopicWord(writePath+"globalTopicWord.txt");

        long endTime = System.currentTimeMillis();
        System.out.println("全局LDA训练完成，耗时：" + (endTime - startTime) + "ms");


        System.out.println();
        System.out.println("-------------开始训练局部LDA---------------");
        startTime = System.currentTimeMillis();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date startDt = sdf.parse(startDate);
        Date endDt = sdf.parse(endDate);
        Calendar rightNow = Calendar.getInstance();
        rightNow.setTime(startDt);

        while(rightNow.getTime().compareTo(endDt) < 0){
            String date1 = sdf.format(rightNow.getTime());
            System.out.println(date1);
            rightNow.add(Calendar.MONTH, 1);
            rightNow.add(Calendar.DAY_OF_MONTH, -1);
            String date2 = sdf.format(rightNow.getTime());
            System.out.println(date2);
            rightNow.add(Calendar.DAY_OF_MONTH, 1);
            System.out.println();

            ReadParseDocs readParseLocal = new ReadParseDocs(W, vocab, tableName);
            readParseLocal.readDocs(date1, date2);
            readParseLocal.parseDocs();
            int MLocal = readParseLocal.getM();
            int[][] docsLocal = readParseLocal.getDocs();
            int[] docLengthLocal = readParseLocal.getDocLength();

            LDA ldaLocal = new LDA(localK, iterNum, localAlpha, localBeta, docsLocal, docLengthLocal, W, MLocal, vocab);
            ldaLocal.initial();
            ldaLocal.gibbsSampling();
            ldaLocal.sortWriteTopicPro(topK, writePath + "local/" + date1 + "SortedTopicPro.txt");
            ldaLocal.writeTopicWord(writePath + "local/" + date1 + "TopicWord.txt");

//            for(String province : provinceDictionary.keySet()){
//                ReadParseDocs readParseLocal = new ReadParseDocs(W, vocab, tableName);
//                readParseLocal.readDocs(date1, date2, province);
//                readParseLocal.parseDocs();
//                int MLocal = readParseLocal.getM();
//                int[][] docsLocal = readParseLocal.getDocs();
//                int[] docLengthLocal = readParseLocal.getDocLength();
//                System.out.println(provinceDictionary.get(province) + " 共" + MLocal + "篇");
//
//                LDA ldaLocal = new LDA(localK, iterNum, localAlpha, localBeta, docsLocal, docLengthLocal, W, MLocal, vocab);
//                ldaLocal.initial();
//                ldaLocal.gibbsSampling();
//                ldaLocal.sortWriteTopicPro(topK, writePath + "local/" + date1 + provinceDictionary.get(province) + "SortedTopicPro.txt");
//                ldaLocal.writeTopicWord(writePath + "local/" + date1 + provinceDictionary.get(province) + "TopicWord.txt");
//            }
        }

        endTime = System.currentTimeMillis();
        System.out.println("局部LDA训练完成，耗时：" + (endTime - startTime) + "ms");
    }





    public void trainBySpark(){
        System.out.println("-------------开始训练全局LDA---------------");
        long startTime = System.currentTimeMillis();

        // load the data
        ReadParseDocs readParse = new ReadParseDocs(W, vocab, tableName);
        readParse.readDocs(startDate, endDate);
        readParse.parseDocs();
        List<int[]> docs = readParse.getSparkDocs();

        // parse the data
        JavaSparkContext sc = SparkContextUtils.getSc();
        JavaRDD<int[]> data = sc.parallelize(docs);
        JavaRDD<Vector> parsedData = data.map(doc -> {
            double[] values = new double[doc.length];
            for (int i = 0; i < doc.length; i++) {
                values[i] = doc[i];
            }
            return Vectors.dense(values);
        });

        // Index documents with unique IDs
        JavaPairRDD<Long, Vector> corpus =
                JavaPairRDD.fromJavaRDD(parsedData.zipWithIndex().map(Tuple2::swap));
        corpus.cache();

        // Cluster the documents into three topics using LDA
//        LDAModel ldaModel = new LDA().setK(5).run(corpus);

//        Matrix topics = ldaModel.topicsMatrix();


        long endTime = System.currentTimeMillis();
        System.out.println("全局LDA训练完成，耗时：" + (endTime - startTime) + "ms");
    }






    public static void main(String[] args) throws IOException, ParseException {
        if(args.length < 9){
            System.out.println("输入参数 tableName, vocabSize, startDate, endDate, globalK, iterNum, topK, writePath, localK");
            System.out.println("建议参数 news 7000 2019-01-01 2020-05-31 200 50 50 /home/scidb/czy/temp/ 50");
            return;
        }
        String tableName = args[0];
        int vocabSize = Integer.parseInt(args[1]);
        String startDate = args[2];
        String endDate = args[3];
        int globalK = Integer.parseInt(args[4]);
        int iterNum = Integer.parseInt(args[5]);
        int topK = Integer.parseInt(args[6]);
        String writePath = args[7];
        int localK = Integer.parseInt(args[8]);

        String sql = "SELECT news_words from " + tableName + " WHERE news_date >= '" + startDate + "' AND news_date <= '" + endDate + "'";
        List<String> vocab = VocabularyConstructor.constructVocabulary(MySQLUtils.getWords(sql));
        Training training = new Training(vocab, vocabSize, startDate, endDate, globalK, iterNum, topK, writePath, localK, tableName);
        training.train();
    }


}
