package com.daslab.czy.SemanticExtractor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.daslab.czy.Utils.MySQLUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import scala.Int;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ETMComputer {
    private String tableName;
    private String readPath;
    private int vocabSize;
    private String startDate;
    private String endDate;
    private int localK;
    private int globalK;
    private String writePath;
    private int topK;
    private double threshhold;
    private int iterNum;

    private float alpha;
    private float beta;//1/10000
    private int W;
    private List<String> vocab;

    private int cellNum;
    private double[][]  minKLDistance;
    private int[][] minKLDistanceGlobalIndex;

    private HashMap<Integer, ArrayList<Float>> topicVector=new HashMap<>();
    private float[][] topicWordPro;
    private int localVectorK;
    private int docDisplayNum;
    private String hBaseLoc;
    private String hBaseUrl;
    private String hBaseTableName;
    private TopicCubeSaver topicCubeSaver;



    public ETMComputer(String tableName,int vocabSize,String startDate,String endDate,int localK,int globalK,String readPath,String writePath,int topK,double threshhold,int iterNum, List<String> vocab, int docDisplayNum, String hBaseLoc, String hBaseUrl, String hBaseTableName) throws IOException {
        this.tableName = tableName;
        this.readPath=readPath;
        this.vocabSize=vocabSize;
        this.startDate = startDate;
        this.endDate = endDate;
        this.localK=localK;
        this.globalK=globalK;
        this.writePath=writePath;
        this.topK=topK;
        this.threshhold=threshhold;
        this.iterNum=iterNum;
        this.docDisplayNum=docDisplayNum;
        this.hBaseLoc = hBaseLoc;
        this.hBaseUrl = hBaseUrl;
        this.hBaseTableName = hBaseTableName;

        this.vocab = vocab.subList(0, Math.min(vocabSize, vocab.size()));
        this.W = this.vocab.size();


        minKLDistance=new double[cellNum][localK];
        minKLDistanceGlobalIndex=new int[cellNum][localK];
        topicCubeSaver = new TopicCubeSaver(hBaseLoc, hBaseUrl, hBaseTableName);
    }

    public float[][] readTopicWord(String filePath,int K,int W)throws IOException {
        float[][] topicWordPro=new float[K][W];
        InputStreamReader inStrR = new InputStreamReader(new FileInputStream(filePath));
        BufferedReader br = new BufferedReader(inStrR);
        String line = br.readLine();
        String[] topicWord;
        int topicIndex;
        while (line != null) {
            topicWord=line.split(":");
            topicIndex=Integer.parseInt(topicWord[0]);
            String[] wordspro=topicWord[1].split(",");
            for(int i=0;i<wordspro.length;i++,i++)
            {
                topicWordPro[topicIndex][Integer .parseInt(wordspro[i])]=Float.parseFloat(wordspro[i+1]);
            }
            line = br.readLine();
        }
        br.close();
        return topicWordPro;
    }

    public void computeExtendedTopicWord() throws IOException, ParseException {
        System.out.println();
        System.out.println("-------------开始计算完整topic集---------------");

        int cellNumTemp=0;
        String globalTopicWordPath=readPath+"globalTopicWord.txt";
        float[][] globalTopicWord=new float[globalK][W];
        globalTopicWord=readTopicWord(globalTopicWordPath, globalK, W);
        for(int i=0;i<globalK;i++)
        {
            ArrayList<Float> topic=new ArrayList<>();
            for(int j=0;j<W;j++)
            {
                topic.add(globalTopicWord[i][j]);
            }
            topicVector.put(i,topic);
        }
        localVectorK=globalK;

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

            String localTopicWordPath=readPath + "local/" + date1 + "TopicWord.txt";
            float[][] localTopicWord=new float[localK][W];
            localTopicWord=readTopicWord(localTopicWordPath, localK, W);
            double[][] distance=new double[localK][topicVector.size()];
            for(int i=0;i<localK;i++)
            {
                for(int j=0;j<topicVector.size();j++)
                {
                    ArrayList<Float> topicArrayList=topicVector.get(j);
                    double sum=0;
                    for(int t=0;t<W;t++)
                    {
                        sum+=localTopicWord[i][t]*Math.log(localTopicWord[i][t]/topicArrayList.get(t))+
                                topicArrayList.get(t)*Math.log(topicArrayList.get(t)/localTopicWord[i][t]);
                    }
                    distance[i][j]=sum/(2);
                }
            }
            for(int i=0;i<localK;i++)
            {
                double[] temp=distance[i];
                int index=0;
                double minDistance = 10000.0;
                for(int j=0;j<temp.length;j++)
                {
                    if(temp[j] < minDistance){
                        minDistance = temp[j];
                        index=j;
                    }
                }
//                System.out.println(minDistance);
//                System.out.println(i);
//                System.out.println(index);

//                minKLDistance[cellNumTemp][i]=temp[0];
                if(temp[index]>=threshhold){
                    ArrayList<Float> topicArrayList=new ArrayList<>();
                    for(int j=0;j<W;j++){
                        topicArrayList.add(localTopicWord[i][j]);
                    }
                    topicVector.put(topicVector.size(), topicArrayList);
//                    minKLDistanceGlobalIndex[cellNumTemp][i]=topicVector.size()-1;
                }
                else{
//                    minKLDistanceGlobalIndex[cellNumTemp][i]=index;
                }
            }
            System.out.println(topicVector.size());
//            cellNumTemp++;
        }
        localVectorK=topicVector.size();
        alpha=(float)1.0/localVectorK;
        beta=(float)1.0/W;
        topicWordPro=new float[localVectorK][W];
        for(int i=0;i<localVectorK;i++)
        {
            ArrayList<Float> topic=topicVector.get(i);
            for(int j=0;j<W;j++)
            {
                topicWordPro[i][j]=topic.get(j);
            }
        }
    }

    public void writeK(String filePath) throws IOException{
        File file=new File(filePath);
        if (!file.exists()) {
            file.createNewFile();
        }
        BufferedWriter writer=new BufferedWriter(new FileWriter(file));
        writer.write(""+localVectorK);
        writer.close();
    }

    public void sortWriteTopicPro(int topK,String filePath) throws IOException{
        File file=new File(filePath);
        if (!file.exists()) {
            file.createNewFile();
        }
        BufferedWriter writer=new BufferedWriter(new FileWriter(file));
        for(int i=0;i<localVectorK;i++)
        {
            writer.write(i+":");
            float[] wordPro=new float[W];
            int[] wordIndex=new int[W];
//			float sum=0;
//			for(int j=0;j<W;j++)
//			{
//				sum+=topic[i][j]+beta;
//			}
            for(int j=0;j<W;j++)
            {
                wordPro[j]=topicWordPro[i][j];
                wordIndex[j]=j;
            }
            float tempFloat;
            int tempInt;
            for(int j=0;j<topK;j++)
            {
                for(int t=j+1;t<W;t++)
                {
                    if(wordPro[j]<wordPro[t])
                    {
                        tempFloat=wordPro[j];
                        wordPro[j]=wordPro[t];
                        wordPro[t]=tempFloat;
                        tempInt=wordIndex[j];
                        wordIndex[j]=wordIndex[t];
                        wordIndex[t]=tempInt;
                    }
                }
            }
            for(int j=0;j<topK;j++)
            {
                writer.write(vocab.get(wordIndex[j])+" "+wordPro[j]+" ");
            }
            writer.write("\n");
        }
        writer.close();
    }

    public void writeTopicWord(String filePath) throws IOException{
        File file=new File(filePath);
        if (!file.exists()) {
            file.createNewFile();
        }
        BufferedWriter writer=new BufferedWriter(new FileWriter(file));

        for(int i=0;i<localVectorK;i++)
        {
            writer.write(i+":");
            float sum=0;
//			for(int j=0;j<W;j++)
//			{
//				sum+=(topicWordNum[i][j]+beta);
//			}
            for(int j=0;j<W;j++)
            {
                writer.write(j+","+(topicWordPro[i][j])+",");
            }
            writer.write("\n");
        }
        writer.close();
    }

    public static Map<String, String[]> readNeighbor(String filePath) throws IOException {
        Map<String, String[]> units = new HashMap<>();
        InputStreamReader inStrR = new InputStreamReader(new FileInputStream(filePath));
        BufferedReader br = new BufferedReader(inStrR);
        String line = br.readLine();
        while (line != null) {
            try{
                String unit = line.split(":")[0];
                String[] neighbors = line.split(":")[1].split(",");
                units.put(unit, neighbors);
            }catch (Exception e){
            }
            line = br.readLine();
        }
        br.close();
        return units;
    }

    public void computeAndSaveDatasetTopicPro() throws IOException, ParseException {
        Map<String, String[]> citys = readNeighbor(readPath + "neighborWithoutOtherPro.txt");
        Map<String, String[]> provinces = readNeighbor(readPath + "chinaNeighbor.txt");

        File file1=new File(readPath+"time.txt");
        if (!file1.exists()) {
            file1.createNewFile();
        }
        BufferedWriter writerTime=new BufferedWriter(new FileWriter(file1));

        File file2=new File(writePath+"M.txt");
        if (!file2.exists()) {
            file2.createNewFile();
        }
        BufferedWriter writerM =new BufferedWriter(new FileWriter(file2));

        long time1=System.currentTimeMillis();
        computeExtendedTopicWord();
        long time2=System.currentTimeMillis();
        writerTime.write("Compute extended topic model time="+(time2-time1)+"\n");

        writeK(writePath+"K.txt");
        writeTopicWord(writePath+"topicWordPro.txt");
        sortWriteTopicPro(topK, writePath+"sortedTopicWordPro.txt");

        File file=new File(writePath+"perplexity.txt");
        if (!file.exists()) {
            file.createNewFile();
        }
        BufferedWriter writer=new BufferedWriter(new FileWriter(file));
//		writer.write(""+localVectorK);

        System.out.println();
        System.out.println("-------------开始训练InferenceLDA---------------");
        long time5=System.currentTimeMillis();

        ReadParseDocs readParse = new ReadParseDocs(W, vocab, tableName);
        readParse.readIdsAndRawDocs(startDate, endDate);
        readParse.parseIdsAndRawDocs();

        int M = readParse.getM();
        int[][] docs = readParse.getDocs();
        int[] docLength = readParse.getDocLength();
        int[] ids = readParse.getIds();
        Map<Integer, Integer> idToPos = readParse.getIdToPos();

        String globalTopicWordPath=writePath+"topicWordPro.txt";
        InferenceLDA inferlda=new InferenceLDA(localVectorK, iterNum, alpha, beta, docs, docLength, W, M, vocab, globalTopicWordPath);
        inferlda.initial();
        inferlda.inferInitial();
        writer.write(inferlda.inferGibbsSampling()+"\n");

        // compute every document's topic distribution
        int[][] docTopicNum = inferlda.getDocTopicNum();
        Map<Integer, List<Double>> docTopicDistribution = new HashMap();
        for(int i = 0; i < M; i++){
            int id = idToPos.get(i);
            List<Double> topicPro = new ArrayList<>();
            for(int j = 0; j < localVectorK; j++){
                double pro = docTopicNum[i][j] == 0 ? 0.0 : (double) docTopicNum[i][j] / docLength[i];
                topicPro.add(pro);
            }
            docTopicDistribution.put(id, topicPro);
        }

        long time6=System.currentTimeMillis();
        System.out.println("InferenceLDA完成，耗时：" + (time6 - time5) + "ms");


        System.out.println("-------------开始保存结果---------------");

        // Save the global cell  ("00")
        saveCell("00", docTopicDistribution, ids);

        // Save all cells
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date startDt = sdf.parse(startDate);
        Date endDt = sdf.parse(endDate);
        Calendar rightNow = Calendar.getInstance();
        rightNow.setTime(startDt);

        SimpleDateFormat monthSdf = new SimpleDateFormat("yyyyMM");
        String rowKey;

        while(rightNow.getTime().compareTo(endDt) < 0) {
            String date1 = sdf.format(rightNow.getTime());
            rightNow.add(Calendar.MONTH, 1);
            rightNow.add(Calendar.DAY_OF_MONTH, -1);
            String date2 = sdf.format(rightNow.getTime());
            rightNow.add(Calendar.DAY_OF_MONTH, 1);

            ReadParseDocs readParseLocal = new ReadParseDocs(W, vocab, tableName);
            readParseLocal.readIdsAndRawDocs(date1, date2);
            readParseLocal.parseIdsAndRawDocs();
            int[] idsLocal = readParseLocal.getIds();

            //save "20XXXX", granularity is MONTH
            rowKey = "20" + monthSdf.format(sdf.parse(date1));
            saveCell(rowKey, docTopicDistribution, idsLocal);


            for(String province : provinces.keySet()){
                ReadParseDocs readParsePro = new ReadParseDocs(W, vocab, tableName);
                readParsePro.readIdsAndRawDocs(date1, date2, province);
                readParsePro.parseIdsAndRawDocs();
                int[] idsPro = readParsePro.getIds();

                //save "22XXXX", granularity is MONTH and Province
                rowKey = "22" + monthSdf.format(sdf.parse(date1)) + province;
                saveCell(rowKey, docTopicDistribution, idsPro);

                for(String city : citys.keySet()){
                    if(!city.startsWith(province)){
                        continue;
                    }
                    ReadParseDocs readParseCity = new ReadParseDocs(W, vocab, tableName);
                    readParseCity.readIdsAndRawDocs(date1, date2, province, city);
                    readParseCity.parseIdsAndRawDocs();
                    int[] idsCity = readParseCity.getIds();

                    //save "21XXXX", granularity is MONTH and City
                    rowKey = "21" + monthSdf.format(sdf.parse(date1)) + city;
                    saveCell(rowKey, docTopicDistribution, idsCity);
                }
            }
        }
    }

    private void saveCell(String rowKey, Map<Integer, List<Double>> docTopicDistribution, int[] idsLocal) {
        Map<Integer, List<Double>> docTopicDistributionLocal = computeDocTopicDistributionLocal(docTopicDistribution, idsLocal);
        List<Double> topicDistribution = computeTopicDistribution(docTopicDistributionLocal);
        List<List<Integer>> postingList = computePostingList(docTopicDistributionLocal);
        int MLocal = idsLocal.length;
        topicCubeSaver.saveToHBase(rowKey, topicDistribution, postingList, MLocal);
    }


    private Map<Integer, List<Double>> computeDocTopicDistributionLocal(Map<Integer, List<Double>> docTopicDistribution, int[] ids) {
        Map<Integer, List<Double>> docTopicDistributionLocal = new HashMap<>();
        for(int id : ids){
            docTopicDistributionLocal.put(id, docTopicDistribution.get(id));
        }
        return docTopicDistributionLocal;
    }

    private List<Double> computeTopicDistribution(Map<Integer, List<Double>> docTopicDistribution) {
        Double[] result = new Double[localVectorK];
        int M = docTopicDistribution.size();
        for(List<Double> topicDis : docTopicDistribution.values()){
            for(int i = 0; i < localVectorK; i++){
                result[i] += topicDis.get(i);
            }
        }
        for(int i = 0; i < localVectorK; i++){
            result[i] /= M;
        }
        return new ArrayList<>(Arrays.asList(result));
    }

    private List<List<Integer>> computePostingList(Map<Integer, List<Double>> docTopicDistribution) {
        List<List<Integer>> result = new ArrayList<>();
        for(int j = 0; j < localVectorK; j++){
            List<Integer> topDoc = getTopDoc(docTopicDistribution, docDisplayNum, j);
            result.add(topDoc);
        }
        return result;
    }

    private List<Integer> getTopDoc(Map<Integer, List<Double>> docTopicDistribution, int docDisplayNum, int topic) {
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


    public static void main(String[] args) throws IOException, ParseException {
        if(args.length != 14){
            System.out.println("Please input : tableName vocabSize startDate endDate globalK iterNum topK localK readPath writePath threshold docDisplayNum hBaseLoc hBaseUrl");
            System.out.println("建议：news 7000 2019-01-01 2020-05-31 200 50 50 50 /home/scidb/czy/readPath/ /home/scidb/czy/writePath/ 3 100 hbase.rootdir hdfs://localhost:9000/hbase");
            return;
        }
        String tableName = args[0];
        int vocabSize = Integer.parseInt(args[1]);
        String startDate = args[2];
        String endDate = args[3];
        int globalK = Integer.parseInt(args[4]);
        int iterNum = Integer.parseInt(args[5]);
        int topK = Integer.parseInt(args[6]);
        int localK = Integer.parseInt(args[7]);
        String readPath = args[8];
        String writePath = args[9];
        double threshold = Double.parseDouble(args[10]);
        int docDisplayNum = Integer.parseInt(args[11]);
        String hBaseLoc = args[12];
        String hBaseUrl = args[13];
        String hBaseTableName = args[14];

        String sql = "SELECT news_words from " + tableName + " WHERE news_date >= '" + startDate + "' AND news_date <= '" + endDate + "'";
        List<String> vocab = VocabularyConstructor.constructVocabulary(MySQLUtils.getWords(sql));
        ETMComputer cetm = new ETMComputer(tableName, vocabSize, startDate, endDate, localK, globalK, readPath, writePath, topK, threshold, iterNum, vocab, docDisplayNum, hBaseLoc, hBaseUrl, hBaseTableName);
        cetm.computeAndSaveDatasetTopicPro();
    }
}
