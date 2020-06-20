package com.daslab.czy.proviceRecognizing;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.daslab.czy.Utils.MySQLUtils;
import com.daslab.czy.Utils.SparkContextUtils;
import com.daslab.czy.model.Token;
import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.corpus.document.sentence.Sentence;
import com.hankcs.hanlp.model.crf.CRFLexicalAnalyzer;
import com.hankcs.hanlp.seg.Segment;
import com.hankcs.hanlp.seg.common.Term;
import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static com.daslab.czy.Utils.MongoDBUtils.getDataFromMongoDB;
import static com.daslab.czy.dataPreprocessing.HanLPPreprocessing.preprocessing;

public class Recognizing2 {
    static Map<String,String> dictionary = initDictionary(); //完全匹配字典
    static List<String> badCities = initBadCities();
    static List<String> badNames = initBadNames();

    static Map<String,String> initDictionary() {
        Map<String,String> result = new HashMap<>();

        String dictionaryName = "/home/leaves1233/IdeaProjects/UniqueTopics/src/main/resources/division_dictionary.json";
//        String dictionaryName = "/home/scidb/czy/resources/division_dictionary.json";
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
            result.put(code, name);
        }
        return result;
    }

    static List<String> initBadCities(){
        List<String> badCities = new ArrayList<>();
        badCities.add("1101");
        badCities.add("1201");
        badCities.add("3101");
        badCities.add("5001");
        badCities.add("4690");
        badCities.add("4290");
        badCities.add("4190");
        return badCities;
    }

    static List<String> initBadNames(){
        List<String> badNames = new ArrayList<>();
        badNames.add("省");
        badNames.add("市");
        badNames.add("县");
        badNames.add("城区");
//        badNames.add("产业园");
//        badNames.add("开发区");
//        badNames.add("经济开发区");
//        badNames.add("技术开发区");
//        badNames.add("经济技术开发区");
//        badNames.add("化工园区");
//        badNames.add("高新技术产业开发区");
        badNames.add("长江");
        badNames.add("中国");
        badNames.add("东");
        badNames.add("南");
        badNames.add("西");
        badNames.add("北");
        return badNames;
    }


    public static List<Document> recognize(List<Document> records){
        int accurate = 0;
        int whole = 0;
        int mistake = 0;
        int miss = 0;
//        try {
//            System.setOut(new PrintStream(new BufferedOutputStream(
//                    new FileOutputStream("src/main/resources/out.txt")),true));
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
        System.out.println();
        System.out.println("------------开始行政区划识别-------------");
        long startTime = System.currentTimeMillis();
        List<Document> result = new ArrayList<>();
        for(Document record : records) {
            String text = (String) record.get("news_text");
            List<Token> tokens = (List<Token>) record.get("tokens");
            List<String> trueLocations = new ArrayList<>();
            for (Token token : tokens) {
                if (token.pos.equals("ns")) {
                    String word = token.word;
                    if (word.charAt(word.length() - 1) == '路' && !word.equals("让胡路")) {
                        continue;
                    } else if (badNames.contains(word)) {
                        continue;
                    } else {
                        trueLocations.add(word);
                    }
                }
            }


            // title部分
            String title = record.getString("news_title");
            List<Token> titleTokens = (List<Token>) record.get("titleTokens");
            List<String> titleLocations = new ArrayList<>();
            for (Token token : titleTokens) {
                if (token.pos.equals("ns")) {
                    String word = token.word;
                    if (word.charAt(word.length() - 1) == '路' && !word.equals("让胡路")) {
                        continue;
                    } else if (badNames.contains(word)) {
                        continue;
                    } else {
                        titleLocations.add(word);
                    }
                }
            }

            Map<String, Double> textScores = computeScores(trueLocations);
            Map<String, Double> titleScores = computeScores(titleLocations);

            Map<String, Double> scores = textScores;

            //添加title权重
            double paramB = 2;
            for(String code : titleScores.keySet()){
                scores.put(code, textScores.getOrDefault(code, (double)0) + paramB * titleScores.get(code));
            }


            //找到分值最大的省，在该省中找到分值最大的市
            Map<String, Double> provinceScores = new HashMap<>();
            Map<String, Double> cityScores = new HashMap<>();
            for(String code : scores.keySet()){
                double score = scores.get(code);
                if(code.length() == 2){
                    provinceScores.put(code, score);
                }else if(code.length() == 4){
                    cityScores.put(code, score);
                }
            }

            int proThreshold = 4; //超过阈值则为综合性文章
            double Bei = 0.4;
            int proCount = 0;
            String province = null;
            String city = null;
            double maxScore = 0.0;
            for (Map.Entry<String, Double> entry : provinceScores.entrySet()) {
                String name = entry.getKey();
                double score = entry.getValue();
                if (score > maxScore) {
                    maxScore = score;
                    province = name;
                }
            }
            double threshold = maxScore * Bei;
            for (double score : provinceScores.values()) {
                if (score > threshold) {
                    proCount++;
                }
            }

            List<Double> citySs = new ArrayList<>();
            maxScore = 0.0;
            int cityThreshold = 4;
            int cityCount = 0;
            if (province != null && proCount < proThreshold) {
                for (Map.Entry<String, Double> entry : cityScores.entrySet()) {
                    String name = entry.getKey();
                    double score = entry.getValue();
                    if (name.substring(0, 2).equals(province)) {
                        citySs.add(score);
                        if (score > maxScore) {
                            maxScore = score;
                            city = name.substring(0, 4);
                        }
                    }
                }
            }
            threshold = maxScore * Bei;
            for (double score : citySs) {
                if (score > threshold) {
                    cityCount++;
                }
            }


            //如果是直辖市或省直辖县
            List<Double> areaSs = new ArrayList<>();
            maxScore = 0.0;
            String area = null;
            int areaThreshold = 5;
            int areaCount = 0;
            if (city != null && badCities.contains(city)) {
                for (Map.Entry<String, Double> entry : scores.entrySet()) {
                    String name = entry.getKey();
                    double score = entry.getValue();
                    if (name.startsWith(city) && !name.equals(city)) {
                        areaSs.add(score);
                        if (score > maxScore) {
                            maxScore = score;
                            area = name;
                        }
                    }
                }
            }
            threshold = maxScore * Bei;
            for (double score : areaSs) {
                if (score > threshold) {
                    areaCount++;
                }
            }

            String locationId = "unknown";
            String childLocationId = "unknown";

            if (province != null) {
                if (proCount >= proThreshold) {
//                    System.out.println("综合性文章，无具体省份");
                } else {
                    locationId = province;
//                    System.out.print("Province： " + dictionary.get(province));
//                    if(!province.equals("31")) System.out.println("BUDUIBUDUI");
                    if (city != null) {
                        if (cityCount >= cityThreshold) {
//                            System.out.println("  綜合性文章，无具体城市");
                        } else {
                            if (badCities.contains(city)) {
                                if (area != null) {
                                    if (areaCount >= areaThreshold) {
//                                        System.out.println("综合性文章，无具体区县");
                                    } else {
                                        childLocationId = area;
//                                        System.out.println("  Area: " + dictionary.get(area));
                                    }
                                }
                            } else {
                                childLocationId = city;
//                                System.out.println("  City： " + dictionary.get(city));
                            }
                        }
                    }
                }
            } else {
//                System.out.println("找不到地址");
            }

            System.out.println(title);
            System.out.println(text);
            System.out.println(trueLocations);
            System.out.println(locationId + " " + childLocationId);

            if(record.containsKey("lc")){
                whole++;
                System.out.println(whole);
                String lc = record.getString("lc");
                String clc = record.getString("clc");
                if(!lc.equals(locationId) || !clc.equals(childLocationId)){
                    System.out.println("不对了");
                    System.out.println(lc + " " + clc);
                    mistake++;
                }
            }


        }

        System.out.println("whole:" + whole);
        System.out.println("mistake:" + mistake);
        return result;
    }

    private static Map<String, Double> computeScores(List<String> locations) {
        Map<String, Double> result = new HashMap<>();

        Map<String, Integer> tf = new HashMap<>();
        for(String location : locations){
            tf.put(location, tf.getOrDefault(location, 0) + 1);
        }

        //计算出每个code的tf
        Map<String, Double> trueTf = new HashMap<>();
        for(String location : tf.keySet()){
            int count = tf.get(location);
            int search = 0;
            Set<Map.Entry<String, String>> entries = dictionary.entrySet();
            List<String> codes = new ArrayList<>();
            List<String> prefixs = new ArrayList<>();
            for (Map.Entry<String, String> entry : entries) {
                String name = entry.getValue();
                String code = entry.getKey();
                if (name.contains(location)) {
                    if(!prefixs.contains(code.substring(0, 2))){
                        codes.add(code);
                        prefixs.add(code.substring(0, 2));
                        search++;
                    }
                }
            }
            double trueCount = (double)count/search;
            for(String code : codes){
                trueTf.put(code, trueTf.getOrDefault(code, (double) 0) + trueCount);
            }
        }

        //通过tf求出初步scores
        Map<String, Double> scores = new HashMap<>();
        for(String code : trueTf.keySet()){
            double count = trueTf.get(code);
            scores.put(code, Math.sqrt(count));
        }

        //“上海市”和“松江区”相互佐证
//        double paramA = 0.3;
//        Map<String, Integer> howManyDependency = new HashMap<>();  //有多少佐证
//        for(String code : scores.keySet()){
//            if(code.length() == 4){
//                String pro = code.substring(0, 2);
//                if(scores.containsKey(pro)){
//                    howManyDependency.put(pro, howManyDependency.getOrDefault(pro, 0) + 1);
//                    howManyDependency.put(code, howManyDependency.getOrDefault(code, 0) + 1);
//                }
//            }else if(code.length() == 6){
//                String pro = code.substring(0, 2);
//                String city = code.substring(0, 4);
//                if(scores.containsKey(pro) && scores.containsKey(city)){
//                    howManyDependency.put(pro, howManyDependency.getOrDefault(pro, 0) + 1);
//                    howManyDependency.put(city, howManyDependency.getOrDefault(city, 0) + 1);
//                    howManyDependency.put(code, howManyDependency.getOrDefault(code, 0) + 1);
//                }else if(scores.containsKey(pro)){
//                    howManyDependency.put(pro, howManyDependency.getOrDefault(pro, 0) + 1);
//                    howManyDependency.put(code, howManyDependency.getOrDefault(code, 0) + 1);
//                }else if(scores.containsKey(city)){
//                    howManyDependency.put(city, howManyDependency.getOrDefault(city, 0) + 1);
//                    howManyDependency.put(code, howManyDependency.getOrDefault(code, 0) + 1);
//                }
//            }
//        }
//        for(String code : howManyDependency.keySet()){
//            int dependencyCount = howManyDependency.get(code);
//            double rawScore = scores.get(code);
//            double newScore = rawScore * (1 + paramA * dependencyCount);
//            scores.put(code, newScore);
//        }

        //将市、县的score加给省、市
        for(String code : scores.keySet()){
            double score = scores.get(code);
            if(code.length() == 2){
                result.put(code, result.getOrDefault(code, (double)0) + score);
            }else if(code.length() == 4){
                String pro = code.substring(0, 2);
                result.put(pro, scores.getOrDefault(pro, (double)0) + score);
                result.put(code, result.getOrDefault(code, (double)0) + score);
            }else if(code.length() == 6){
                String pro = code.substring(0, 2);
                String city = code.substring(0, 4);
                result.put(pro, scores.getOrDefault(pro, (double)0) + score);
                result.put(city, scores.getOrDefault(city, (double)0) + score);
                result.put(code, result.getOrDefault(code, (double)0) + score);
            }
        }

        return result;
    }

    public static List<Document> preprocessing(List<Document> records){
        System.out.println();
        System.out.println("---------------开始预处理--------------");
        long startTime = System.currentTimeMillis();
        JavaSparkContext sc = SparkContextUtils.getSc();
        JavaRDD<Document> rdd = sc.parallelize(records);
        JavaRDD<Document> rdd1 = rdd.map((Document d) -> {
            String text = (String) d.get("news_text");
            String title = d.getString("news_title");

            //切text
            text = text.substring(10);
            Segment segment = HanLP.newSegment();
            List<Term> termList = segment.seg(text);
//            CRFLexicalAnalyzer analyzer = new CRFLexicalAnalyzer();
//            Sentence analyze = analyzer.analyze(text);
            List<Token> tokens = new ArrayList<>();
            for (Term term : termList) {
                tokens.add(new Token(term.word, term.nature.toString()));
            }
            d.put("tokens", tokens);

            //切title
            Segment titleSegment = HanLP.newSegment();
            List<Term> titleTermList = titleSegment.seg(title);
            List<Token> titleTokens = new ArrayList<>();
            for(Term term : titleTermList){
                titleTokens.add(new Token(term.word, term.nature.toString()));
            }
            d.put("titleTokens", titleTokens);

            return d;
        });
        List<Document> result = rdd1.collect();
        long endTime = System.currentTimeMillis();
        System.out.println("预处理完成，耗时：" + (endTime - startTime) + "ms");
        return result;
    }



    public static void main(String[] args) {
        if(args.length < 1){
            System.out.println("输入mongodb的表名");
            return;
        }
        String collection = args[0];
        List<Document> records= getDataFromMongoDB("10.131.238.35", "Chinanews", new String[]{collection});
        records = preprocessing(records);
        records = recognize(records);

    }
}
