package com.daslab.czy.SemanticExtractor;

import com.daslab.czy.Utils.MySQLUtils;

import java.util.ArrayList;
import java.util.List;

public class ReadParseDocs {
    private List<String> vocab = new ArrayList<>();
    private List<String> docName = new ArrayList<>();
    private List<String> rawDocs;
    private int[][] docs;
    private List<int[]> sparkDocs;
    private int[] docLength;
    private int W;
    private int M;
    private String tableName;

    public int getM() {
        return M;
    }

    public List<String> getVocab() {
        return vocab;
    }

    public List<String> getDocName() {
        return docName;
    }

    public List<String> getRawDocs() {
        return rawDocs;
    }

    public List<int[]> getSparkDocs() {
        return sparkDocs;
    }

    public int[][] getDocs() {
        return docs;
    }

    public int[] getDocLength() {
        return docLength;
    }

    public int getW() {
        return W;
    }

    public String getTableName() {
        return tableName;
    }

    public ReadParseDocs(int W, List<String> vocab, String tableName){
        this.W = W;
        this.vocab = vocab;
        this.tableName = tableName;
    }

    public void readDocs(String startDate, String endDate){
        String sql = "SELECT news_words from " + tableName + " WHERE news_date >= '" + startDate + "' AND news_date <= '" + endDate + "'";
        this.rawDocs = MySQLUtils.getWords(sql);
    }

    public void readDocs(String startDate, String endDate, String location){
        String sql = "SELECT news_words from " + tableName + " WHERE news_date >= '" + startDate + "' AND news_date <= '" + endDate + "' AND news_location = '" + location + "'";
        this.rawDocs = MySQLUtils.getWords(sql);
    }

    public void readDocs(String startDate, String endDate, String location, String childLocation){
        String sql = "SELECT news_words from " + tableName + " WHERE news_date >= '" + startDate + "' AND news_date <= '" + endDate + "' AND news_location = '" + location + "' AND news_child_location = '" + childLocation + "'";
        this.rawDocs = MySQLUtils.getWords(sql);
    }

    public void parseDocs(){
        this.M = rawDocs.size();
        this.sparkDocs = new ArrayList<>();
        this.docs = new int[M][];
        this.docLength = new int[M];
        for(int i = 0; i < M; i++){
            String[] words = rawDocs.get(i).split(" ");
            List<Integer> wordsIndex = new ArrayList<>();
            for(String word : words){
                if(this.vocab.contains(word)){
                    wordsIndex.add(vocab.indexOf(word));
                }
            }
            docLength[i] = wordsIndex.size();
            int[] doc = new int[wordsIndex.size()];
            for(int j = 0; j < wordsIndex.size(); j++){
                doc[j] = wordsIndex.get(j);
            }
            docs[i] = doc;
            sparkDocs.add(doc);
        }
    }


}
