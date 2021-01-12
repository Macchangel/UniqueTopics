package com.daslab.czy;






import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.daslab.czy.model.Token;
import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.corpus.document.sentence.Sentence;
import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary;
import com.hankcs.hanlp.model.crf.CRFLexicalAnalyzer;
import com.hankcs.hanlp.seg.Segment;
import com.hankcs.hanlp.seg.common.Term;
import org.apache.spark.sql.sources.In;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Test {

    public static void main(String[] args) throws ParseException, IOException {

        String startDate = "2019-02-23";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date startDt = sdf.parse(startDate);

        SimpleDateFormat saveSdf = new SimpleDateFormat("yyyyMM");
        String format = saveSdf.format(startDt);
        System.out.println(format);
    }

    static private List<Integer> getTopDoc(Map<Integer, List<Double>> docTopicDistribution, int docDisplayNum, int topic) {
        List<Integer> result = new ArrayList<>();
        List<double[]> idsAndPros = new ArrayList<>();
        for(Map.Entry<Integer, List<Double>> entry : docTopicDistribution.entrySet()){
            idsAndPros.add(new double[]{entry.getKey(), entry.getValue().get(topic)});
        }
        if(docDisplayNum >= idsAndPros.size()){
            Collections.sort(idsAndPros, (o1, o2) -> (Double.compare(o2[1], o1[1])));
            for(double[] item : idsAndPros){
                result.add((int)item[0]);
            }
            return result;
        }else {
            PriorityQueue<double[]> pq = new PriorityQueue<>(docDisplayNum, (o1, o2) -> (Double.compare(o1[1], o2[1])));
            for(double[] item : idsAndPros){
                if(pq.size() < docDisplayNum){
                    pq.offer(item);
                }else {
                    if(item[1] > pq.peek()[1]){
                        pq.poll();
                        pq.offer(item);
                    }
                }
            }
            while (!pq.isEmpty()){
                result.add((int)pq.poll()[0]);
            }
            Collections.reverse(result);
            return result;
        }

    }




}
