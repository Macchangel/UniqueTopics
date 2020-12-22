package com.daslab.czy.proviceRecognizing;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.daslab.czy.Utils.MySQLUtils;
import com.daslab.czy.Utils.SparkContextUtils;
import com.daslab.czy.model.Token;
import com.sun.jersey.api.core.ClasspathResourceConfig;
import javafx.util.Pair;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;

import static com.daslab.czy.Utils.HBaseUtils.saveToHBase;
import static com.daslab.czy.Utils.MongoDBUtils.getDataFromMongoDB;
import static com.daslab.czy.dataPreprocessing.HanLPPreprocessing.preprocessing;

public class Recognizing {
    static Map<String,String> dictionary = initDictionary(); //完全匹配字典
    static List<String> badCities = initBadCities();
    static List<String> badNames = initBadNames();

    // Initialize the division_dictionary
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

    private static int compareTwoCodes(String code1, String code2){
        if(code1.length() == 2){
            if(code1.equals(code2.substring(0, 2))){
                return 1;
            }else {
                return 0;
            }
        }else{
            int ans = 0;
            if(code1.substring(0, 2).equals(code2.substring(0, 2))){
                ans++;
            }
            if(code1.substring(2, 4).equals(code2.substring(2, 4))){
                ans++;
            }
            return ans;
        }
    }

    // recognize函数在Spark平台下的运行版本，在数据量大的时候会出现bug
    public static List<Document> recognize1(List<Document> records){
//        try {
//            System.setOut(new PrintStream(new BufferedOutputStream(
//                    new FileOutputStream("src/main/resources/out.txt")),true));
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
        System.out.println();
        System.out.println("------------开始行政区划识别-------------");
        long startTime = System.currentTimeMillis();
        JavaSparkContext sc = SparkContextUtils.getSc();
        JavaRDD<Document> rdd = sc.parallelize(records);
        JavaRDD<Document> rdd1 = rdd.map((Document record) -> {
            String text = (String) record.get("news_text");
            List<Token> tokens = (List<Token>) record.get("tokens");
            List<Token> rawLocations = new ArrayList<>();
            List<Token> trueLocations = new ArrayList<>();
            List<Token> roads = new ArrayList<>();
            for (Token token : tokens) {
                if (token.pos.equals("ns")) {
                    rawLocations.add(token);
                }
            }
            for (int i = 0; i < rawLocations.size(); i++) {
                Token token = rawLocations.get(i);
                while (i + 1 < rawLocations.size()) {
                    Token next = rawLocations.get(i + 1);
                    if (token.start <= next.start && token.end >= next.end) {
                        i++;
                    } else {
                        break;
                    }
                }
                String word = token.word;
                if (word.charAt(word.length() - 1) == '路' && !word.equals("让胡路")) {
                    roads.add(token);
                } else if (badNames.contains(word)) {
                    continue;
                } else {
                    trueLocations.add(token);
                }
            }
//            System.out.println(trueLocations);

//            Map<Token,List<Pair<String,Double>>> scores = new HashMap<>();
//            for(Token token : trueLocations){
//                String word = token.word;
//                List<Pair<String,Double>> scoreList = new ArrayList<>();
//                Set<Map.Entry<String, String>> entries = dictionary.entrySet();
//                for(Map.Entry<String, String> entry : entries){
//                    String name = entry.getValue();
//                    if(name.contains(word)){
//                        if(name.equals(word)){
//                            scoreList.add(new Pair<>(entry.getKey(), 0.9));
//                        }else{
//                            double score = (double)word.length() / name.length();
//                            scoreList.add(new Pair<>(entry.getKey(), score));
//                        }
//                    }
//                }
//                if(!scoreList.isEmpty()){
//                    scores.put(token, scoreList);
//                }
//            }
//            System.out.println(scores);
//
//            for(Map.Entry<Token,List<Pair<String,Double>>> entry : scores.entrySet()){
//                Token token = entry.getKey();
//                List<Pair<String,Double>> scoreList = entry.getValue();
//                for(Pair<String,Double> pair : scoreList){
//
//                }
//            }


            Map<String, Double> scores = new HashMap<>();
            for (Token token : trueLocations) {
                String word = token.word;
                Set<Map.Entry<String, String>> entries = dictionary.entrySet();
                for (Map.Entry<String, String> entry : entries) {
                    String name = entry.getValue();
                    String code = entry.getKey();
                    if (name.contains(word)) {
                        double score = 0.0;
                        if (name.equals(word)) {
                            score = 0.9;
                        } else {
                            score = (double) word.length() / name.length();
                        }
                        score = (double) (text.length() - token.start) / text.length() * score;
                        scores.put(code, scores.getOrDefault(code, 0.0) + score);
                    }
                }
            }


//            List<String> codes = new ArrayList<>(scores.keySet());
//            Collections.sort(codes);
//            System.out.println(codes);
//            for(int i = 0; i < codes.size(); i++){
//                String code1 = codes.get(i);
//                for(int j = i; j < codes.size(); j++){
//                    String code2 = codes.get(j);
//                    if(!code1.substring(0, 2).equals(code2.substring(0, 2))){
//                        break;
//                    }
//                    int similarity = compareTwoCodes(code1, code2);
//                }
//            }

            Map<String, Double> provinceScores = new LinkedHashMap<>();
            Map<String, Double> cityScores = new HashMap<>();
            for (Map.Entry<String, Double> entry : scores.entrySet()) {
                String name = entry.getKey();
                double score = entry.getValue();
//                System.out.print(dictionary.get(name) + ":" + score + " ");
                if (name.length() == 2) {
                    provinceScores.put(name, (provinceScores.getOrDefault(name, 0.0) + score));
                } else if(name.length() == 4){
                    double alpha = 0.8;  //传递系数
                    String provinceName = name.substring(0, 2);
                    String cityName = name.substring(0, 4);
                    provinceScores.put(provinceName, (provinceScores.getOrDefault(provinceName, 0.0) + score * alpha));
                    cityScores.put(cityName, (cityScores.getOrDefault(cityName, 0.0) + score));
                }else{
                    double alpha = 0.6;  //传递系数
                    double beta = 0.8;
                    String provinceName = name.substring(0, 2);
                    String cityName = name.substring(0, 4);
                    provinceScores.put(provinceName, (provinceScores.getOrDefault(provinceName, 0.0) + score * alpha));
                    cityScores.put(cityName, (cityScores.getOrDefault(cityName, 0.0) + score * beta));
                }
            }

//            for(Map.Entry<String,Double> entry : cityScores.entrySet()){
//                String name = entry.getKey();
//                double score = entry.getValue();
//                String provinceName = name.substring(0, 2);
//                if(provinceScores.containsKey(provinceName)){
//                    provinceScores.put(provinceName, (provinceScores.get(provinceName) + score));
//                }
//            }

//            System.out.println(provinceScores);
//            System.out.println(cityScores);

            //找到分值最大的省，在该省中找到分值最大的市
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

            record.put("news_location", locationId);
            record.put("news_child_location", childLocationId);

            return record;
        });

        List<Document> result = rdd1.collect();
        long endTime = System.currentTimeMillis();
        System.out.println("文章归属地识别完成，耗时：" + (endTime - startTime) + "ms");
        return result;
    }

    /**
     *  识别新闻所属的行政区划
     * @param records 预处理之后存储在mongoDB中的记录
     * @return 行政区划识别后得到的记录
     */
    public static List<Document> recognize(List<Document> records) throws IOException {
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
        for(Document record : records){
            String text = (String) record.get("news_text");  // 取出记录中的news_text属性，即新闻的正文
            List<Token> tokens = (List<Token>) record.get("tokens"); // 取出记录中的token属性，即分词与词性标注的结果
            List<Token> rawLocations = new ArrayList<>();
            List<Token> trueLocations = new ArrayList<>();
            List<Token> roads = new ArrayList<>();
            // 取出词性为ns的词，即地名实体，保存在rawLocations中
            for (Token token : tokens) {
                if (token.pos.equals("ns")) {
                    rawLocations.add(token);
                }
            }
            // 识别出路名，并单独保存，剩下的地名实体保存在trueLocations中
            for (int i = 0; i < rawLocations.size(); i++) {
                Token token = rawLocations.get(i);
                while (i + 1 < rawLocations.size()) {
                    Token next = rawLocations.get(i + 1);
                    if (token.start <= next.start && token.end >= next.end) {
                        i++;
                    } else {
                        break;
                    }
                }
                String word = token.word;
                if (word.charAt(word.length() - 1) == '路' && !word.equals("让胡路")) {
                    roads.add(token);
                } else if (badNames.contains(word)) {
                    continue;
                } else {
                    trueLocations.add(token);
                }
            }
//            System.out.println(trueLocations);

//            Map<Token,List<Pair<String,Double>>> scores = new HashMap<>();
//            for(Token token : trueLocations){
//                String word = token.word;
//                List<Pair<String,Double>> scoreList = new ArrayList<>();
//                Set<Map.Entry<String, String>> entries = dictionary.entrySet();
//                for(Map.Entry<String, String> entry : entries){
//                    String name = entry.getValue();
//                    if(name.contains(word)){
//                        if(name.equals(word)){
//                            scoreList.add(new Pair<>(entry.getKey(), 0.9));
//                        }else{
//                            double score = (double)word.length() / name.length();
//                            scoreList.add(new Pair<>(entry.getKey(), score));
//                        }
//                    }
//                }
//                if(!scoreList.isEmpty()){
//                    scores.put(token, scoreList);
//                }
//            }
//            System.out.println(scores);
//
//            for(Map.Entry<Token,List<Pair<String,Double>>> entry : scores.entrySet()){
//                Token token = entry.getKey();
//                List<Pair<String,Double>> scoreList = entry.getValue();
//                for(Pair<String,Double> pair : scoreList){
//
//                }
//            }


            // 给每一个地名实体与行政区划词典进行比较，赋初始分数
            Map<String, Double> scores = new HashMap<>();
            for (Token token : trueLocations) {
                String word = token.word;
                Set<Map.Entry<String, String>> entries = dictionary.entrySet();
                for (Map.Entry<String, String> entry : entries) {
                    String name = entry.getValue();
                    String code = entry.getKey();
                    if (name.contains(word)) {
                        double score = 0.0;
                        if (name.equals(word)) {
                            score = 0.9;
                        } else {
                            score = (double) word.length() / name.length();
                        }
                        score = (double) (text.length() - token.start) / text.length() * score;
                        scores.put(code, scores.getOrDefault(code, 0.0) + score);
                    }
                }
            }


            // 如果scores为空，说明没有在行政区划词典中找到对应的地名，这时候就利用百度地图api进行查找。
            if(scores.isEmpty() && !trueLocations.isEmpty()){
                System.out.println("使用百度Api");
                for(Token token : trueLocations){
                    String location = token.word;
                    Map<String, String> dataFromAPI = getDataByBaiduAPI(location);
                    String name = dataFromAPI.get("name");
                    String province = dataFromAPI.get("province");
                    String city = dataFromAPI.get("city");
                    String area = dataFromAPI.get("area");
                    if(dictionary.containsKey(name)){
                        scores.put(dictionary.get(name), 0.9);
                    }else if(dictionary.containsKey(province)){
                        String code = dictionary.get(province);
                        scores.put(code, 0.9);
                        if(badCities.contains(code)){
                            scores.put(dictionary.get(area), 0.9);
                        }else {
                            scores.put(dictionary.get(city), 0.9);
                        }
                    }
                }
            }


//            List<String> codes = new ArrayList<>(scores.keySet());
//            Collections.sort(codes);
//            System.out.println(codes);
//            for(int i = 0; i < codes.size(); i++){
//                String code1 = codes.get(i);
//                for(int j = i; j < codes.size(); j++){
//                    String code2 = codes.get(j);
//                    if(!code1.substring(0, 2).equals(code2.substring(0, 2))){
//                        break;
//                    }
//                    int similarity = compareTwoCodes(code1, code2);
//                }
//            }

            // 传递分数
            Map<String, Double> provinceScores = new LinkedHashMap<>();
            Map<String, Double> cityScores = new HashMap<>();
            for (Map.Entry<String, Double> entry : scores.entrySet()) {
                String name = entry.getKey();
                double score = entry.getValue();
//                System.out.print(dictionary.get(name) + ":" + score + " ");
                if (name.length() == 2) {
                    provinceScores.put(name, (provinceScores.getOrDefault(name, 0.0) + score));
                } else if(name.length() == 4){
                    double alpha = 0.8;  //传递系数
                    String provinceName = name.substring(0, 2);
                    String cityName = name.substring(0, 4);
                    provinceScores.put(provinceName, (provinceScores.getOrDefault(provinceName, 0.0) + score * alpha));
                    cityScores.put(cityName, (cityScores.getOrDefault(cityName, 0.0) + score));
                }else{
                    double alpha = 0.6;  //传递系数
                    double beta = 0.8;
                    String provinceName = name.substring(0, 2);
                    String cityName = name.substring(0, 4);
                    provinceScores.put(provinceName, (provinceScores.getOrDefault(provinceName, 0.0) + score * alpha));
                    cityScores.put(cityName, (cityScores.getOrDefault(cityName, 0.0) + score * beta));
                }
            }

//            for(Map.Entry<String,Double> entry : cityScores.entrySet()){
//                String name = entry.getKey();
//                double score = entry.getValue();
//                String provinceName = name.substring(0, 2);
//                if(provinceScores.containsKey(provinceName)){
//                    provinceScores.put(provinceName, (provinceScores.get(provinceName) + score));
//                }
//            }

//            System.out.println(provinceScores);
//            System.out.println(cityScores);

            //找到分值最大的省，在该省中找到分值最大的市
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

            record.put("news_location", locationId);
            record.put("news_child_location", childLocationId);

            result.add(record);
        };

        long endTime = System.currentTimeMillis();
        System.out.println("文章归属地识别完成，耗时：" + (endTime - startTime) + "ms");
        return result;
    }

    // 通过百度API获取每个地名实体所对应的行政区划
    private static Map<String, String> getDataByBaiduAPI(String location) throws IOException {
        URL url = new URL("http://api.map.baidu.com/place/v2/search?query=" + location + "&region=%E5%85%A8%E5%9B%BD&output=json&ak=rKjINv2twq4RVqooCO0iLgT152EilQSg");
        System.out.println("url: " + url);
        HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
        urlConn.setConnectTimeout(50000);
        urlConn.setReadTimeout(50000);
        urlConn.setDoInput(true);
        urlConn.setUseCaches(false);
        urlConn.setRequestProperty("User-agent", "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/13.0.782.215 Safari/535.1");
        urlConn.setRequestProperty("token", "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJrdGJpIiwicm9sZSI6IlJPTEVfVVNFUiIsImlzcyI6InRjbGVyIiwiZXhwIjoxNTk5ODA3Mzc5LCJpYXQiOjE1OTkyMDI1Nzl9.IciVkGcsYfD9hDacV4-lKkGFft-Z-LnEkmYtcDlPjYeN2styo3IA6dbE0JP08bmy8uS8sy3TL_65_fTbEoilow");
        urlConn.setRequestProperty("Content-type", "application/x-java-serialized-object");
        int code = urlConn.getResponseCode();//获得相应码
        System.out.println("请求响应码:"+ code);
        // 设置所有的http连接是否自动处理重定向；设置成true，系统自动处理重定向
        urlConn.setInstanceFollowRedirects(true);
        // 设置所有的http连接是否自动处理重定向；设置成true，系统自动处理重定向
        urlConn.setInstanceFollowRedirects(true);
        // 存储返回的字符串
        String data = "";
        //得到数据流（输入流）
        InputStream is = urlConn.getInputStream();
        byte[] buffer = new byte[1024];
        int length = 0;

        while ((length = is.read(buffer)) != -1) {
            String str = new String(buffer, 0, length);
            data += str;
        }
        System.out.println(data);
        JSONArray results = JSON.parseObject(data).getJSONArray("results");
        String name = "";
        String province = "";
        String city = "";
        String area = "";
        for(int i = 0; i < results.size(); i++){
            JSONObject result = results.getJSONObject(i);
            name = result.getString("name");
            province = result.getString("province");
            city = result.getString("city");
            area = result.getString("area");
            System.out.println(name + " " + province + " " + city + " " + area);
            break;
        }
        Map<String, String> res = new HashMap<>();
        res.put("name", name);
        res.put("province", province);
        res.put("city", city);
        res.put("area", area);
        return res;
    }



    public static void main(String[] args) throws IOException {
        if(args.length < 2){
            System.out.println("输入mongodb的表名和Mysql的表名");
        }

//        String collection = args[0];
//        String tableName = args[1];

        String collection = "Hb";

        List<Document> records= getDataFromMongoDB("127.0.0.1", "Chinanews", new String[]{collection});
        System.out.println("amount:" + records.size());
        records = preprocessing(records);
        System.out.println("amount:" + records.size());
        System.out.println(records.get(0));
        records = recognize(records);
        System.out.println("amount:" + records.size());
        System.out.println(records.get(0));
//        MySQLUtils.insert(records, tableName);
    }
}