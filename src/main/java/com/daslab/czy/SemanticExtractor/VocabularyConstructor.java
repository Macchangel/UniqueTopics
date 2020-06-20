package com.daslab.czy.SemanticExtractor;

import com.daslab.czy.Utils.MySQLUtils;
import com.daslab.czy.Utils.SparkContextUtils;
import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary;
import com.hankcs.hanlp.seg.Segment;
import com.hankcs.hanlp.seg.common.Term;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * 构建词表，根据tfidf排序
 */
public class VocabularyConstructor {

    public static List<String> constructVocabulary(List<String> news_words_list){
        System.out.println();
        System.out.println("---------------开始构建词表--------------");
        long startTime = System.currentTimeMillis();

        HashMap<String, Integer> vocab = new HashMap<>();
        HashMap<String, Integer> idf = new HashMap<>();


        for(String news_words : news_words_list){
            String[] words = news_words.split(" ");
            Set<String> uniqueWords = new HashSet<>();
            for(String word : words){
                if (word.length() < 2){
                    continue;
                }
                vocab.put(word, vocab.getOrDefault(word, 0) + 1);
                uniqueWords.add(word);
            }
            for(String uniqueWord : uniqueWords){
                idf.put(uniqueWord, idf.getOrDefault(uniqueWord, 0) + 1);
            }
        };

        Map<String, Double> tfidf = new HashMap<>();
        int docsNum = news_words_list.size();
        int wordSum = 0;
        for(int wordCount : vocab.values()){
            wordSum += wordCount;
        }
        for(String word : vocab.keySet()){
            double temp = ((double)vocab.get(word) / wordSum) * Math.log((double)docsNum / idf.get(word));
            tfidf.put(word, temp);
        }

        List<Map.Entry<String, Double>> tfidf1 = new ArrayList<>(tfidf.entrySet());
        Collections.sort(tfidf1, ((o1, o2) -> o2.getValue().compareTo(o1.getValue())));

        List<String> result = new ArrayList<>();
        for(Map.Entry<String, Double> entry : tfidf1){
            result.add(entry.getKey());
        }

        long endTime = System.currentTimeMillis();
        System.out.println("词表构建完成，耗时：" + (endTime - startTime) + "ms");
        return result;
    }

    public static void saveVocab(List<String> vocab, String vocabPath) throws IOException {
        File file=new File(vocabPath);
        if (!file.exists()) {
            file.createNewFile();
        }
        BufferedWriter writer=new BufferedWriter(new FileWriter(file));
        for (String word : vocab) {
            writer.write(word+"\n");
        }
        writer.close();
    }


    public static void main(String[] args) throws IOException {
        String tableName = "news";
        String startDate = "2019-01-01";
        String endDate = "2020-05-31";
        String sql = "SELECT news_words from " + tableName + " WHERE news_date >= '" + startDate + "' AND news_date <= '" + endDate + "'";
        List<String> vocab = VocabularyConstructor.constructVocabulary(MySQLUtils.getWords(sql));
        saveVocab(vocab, "/home/leaves1233/IdeaProjects/UniqueTopics/temp/vocab.txt");
    }
}
