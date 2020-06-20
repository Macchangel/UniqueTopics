package com.daslab.czy.SemanticExtractor;

import com.daslab.czy.Utils.MySQLUtils;

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

    public ETMComputer(String tableName,int vocabSize,String startDate,String endDate,int localK,int globalK,String readPath,String writePath,int topK,double threshhold,int iterNum, List<String> vocab) throws IOException {
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

        this.vocab = vocab.subList(0, Math.min(vocabSize, vocab.size()));
        this.W = this.vocab.size();


        minKLDistance=new double[cellNum][localK];
        minKLDistanceGlobalIndex=new int[cellNum][localK];
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

    public void computeDatasetTopicPro() throws IOException, ParseException {
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

        long time3=System.currentTimeMillis();
        System.out.println();
        System.out.println("-------------开始训练全局InferenceLDA---------------");


//        ReadParseDocs readParse = new ReadParseDocs(W, vocab, tableName);
//        readParse.readDocs(startDate, endDate);
//        readParse.parseDocs();
//
//        long time4=System.currentTimeMillis();
//        writerTime.write("read global dataset time="+(time4-time3)+"\n");
//
//        int M=readParse.getM();
//        int[][] docs=readParse.getDocs();
//        int[] docLength=readParse.getDocLength();
//
//        writerM.write("global:" + M);
//        writerM.write("\n");
//
//        writer.write(M+"\t");
//
//        long time5=System.currentTimeMillis();
        String globalTopicWordPath=writePath+"topicWordPro.txt";
//        InferenceLDA inferlda=new InferenceLDA(localVectorK, iterNum, alpha, beta, docs, docLength, W, M, vocab, globalTopicWordPath);
//        inferlda.initial();
//        inferlda.inferInitial();
//        writer.write(inferlda.inferGibbsSampling()+"\n");
//        long time6=System.currentTimeMillis();
//        writerTime.write("global dataset inferLDA time="+(time6-time5)+"\n");
//        inferlda.writeDatasetTopicPro(writePath+"globalDatasetTopicPro.txt");


        System.out.println();
        System.out.println("-------------开始训练局部InferenceLDA---------------");

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date startDt = sdf.parse(startDate);
        Date endDt = sdf.parse(endDate);
        Calendar rightNow = Calendar.getInstance();
        rightNow.setTime(startDt);



        while(rightNow.getTime().compareTo(endDt) < 0) {
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

            writerM.write(date1 + ":" + MLocal);
            writerM.write("\n");

            InferenceLDA inferldaLocal = new InferenceLDA(localVectorK, iterNum, alpha, beta, docsLocal, docLengthLocal, W, MLocal, vocab, globalTopicWordPath);
            inferldaLocal.initial();
            inferldaLocal.inferInitial();
            inferldaLocal.inferGibbsSampling();
            inferldaLocal.writeDatasetTopicPro(writePath + date1 + "DatasetTopicPro.txt");

//            for(String province : provinces.keySet()){
//                ReadParseDocs readParsePro = new ReadParseDocs(W, vocab, tableName);
//                readParsePro.readDocs(date1, date2, province);
//                readParsePro.parseDocs();
//                int MPro=readParsePro.getM();
//                int[][] docsPro=readParsePro.getDocs();
//                int[] docLengthPro=readParsePro.getDocLength();
//
//                writerM.write(province + ":" + MPro);
//                writerM.write("\n");
//
//                InferenceLDA inferldaPro=new InferenceLDA(localVectorK, iterNum, alpha, beta, docsPro, docLengthPro, W, MPro, vocab, globalTopicWordPath);
//                inferldaPro.initial();
//                inferldaPro.inferInitial();
//                inferldaPro.inferGibbsSampling();
//                inferldaPro.writeDatasetTopicPro(writePath + "province/" + date1 + province + "DatasetTopicPro.txt");
//
//                for(String city : citys.keySet()){
//                    if(!city.startsWith(province)){
//                        continue;
//                    }
//                    ReadParseDocs readParseCity = new ReadParseDocs(W, vocab, tableName);
//                    readParseCity.readDocs(date1, date2, province, city);
//                    readParseCity.parseDocs();
//                    int MCity = readParseCity.getM();
//                    int[][] docsCity = readParseCity.getDocs();
//                    int[] docLengthCity = readParseCity.getDocLength();
//
//                    writerM.write(city + ":" + MCity);
//                    writerM.write("\n");
//
//                    InferenceLDA inferldaCity = new InferenceLDA(localVectorK, iterNum, alpha, beta, docsCity, docLengthCity, W, MCity, vocab, globalTopicWordPath);
//                    inferldaCity.initial();
//                    inferldaCity.inferInitial();
//                    inferldaCity.inferGibbsSampling();
//                    inferldaCity.writeDatasetTopicPro(writePath + "city/" + date1 + city + "DatasetTopicPro.txt");
//                }
//            }
        }
        writer.close();
        writerTime.close();
        writerM.close();
    }


    public static void main(String[] args) throws IOException, ParseException {
        if(args.length != 11){
            System.out.println("建议：news 7000 2019-01-01 2020-05-31 200 50 50 50 /home/scidb/czy/readPath/ /home/scidb/czy/writePath/ 3");
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

        String sql = "SELECT news_words from " + tableName + " WHERE news_date >= '" + startDate + "' AND news_date <= '" + endDate + "'";
        List<String> vocab = VocabularyConstructor.constructVocabulary(MySQLUtils.getWords(sql));
        ETMComputer cetm = new ETMComputer(tableName, vocabSize, startDate, endDate, localK, globalK, readPath, writePath, topK, threshold, iterNum, vocab);
        cetm.computeDatasetTopicPro();
    }
}
