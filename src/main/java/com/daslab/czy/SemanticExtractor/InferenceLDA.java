package com.daslab.czy.SemanticExtractor;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class InferenceLDA {
	private int K;
	private int iterNum;
	private float alpha;
	private float beta;
	private int[][] docs;
	private int[] docLength;
	private int W;
	private int M;
	private List<String> vocab=new ArrayList<>();
	private String topicModelPath;
	private int[][] docWordTopic;
	private int[][] docTopicNum;
	private float[][] topicWordPro;
	
	public InferenceLDA(){
		super();
	}
	public InferenceLDA(int K, int iterNum, float alpha, float beta, int[][] docs, int[] docLength, int W, int M, List<String> vocab, String topicModelPath){
		super();
		this.K=K;
		this.iterNum=iterNum;
		this.alpha=alpha;
		this.beta=beta;
		this.docs=docs;
		this.docLength=docLength;
		this.W=W;
		this.M=M;
		this.vocab=vocab;
		
		topicWordPro=new float[K][W];
		this.topicModelPath=topicModelPath;
		
		docTopicNum=new int[M][K];
		docWordTopic=new int[M][];
		for(int i=0;i<M;i++)
		{
			docWordTopic[i]=new int[docLength[i]];
		}
	}

	public void initial() throws IOException{
		for(int i=0;i<M;i++)
		{
			for(int j=0;j<docLength[i];j++)
			{
				docWordTopic[i][j]=0;
			}
		}
		for(int i=0;i<M;i++)
		{
			for(int j=0;j<K;j++)
			{
				docTopicNum[i][j]=0;
			}
		}
		topicWordPro=readTopicWord(topicModelPath);
	}

	public double perplexity()
	{
		double perplexity=0;
		double nume_sum=0;
		double deno_sum=0;
		int topic;
		double p;
		for(int i=0;i<M;i++)
		{
			deno_sum+=docLength[i];
			for(int j=0;j<docLength[i];j++)
			{
				topic=docWordTopic[i][j];
				p=((docTopicNum[i][topic]+alpha)/(docLength[i]+K*alpha))*topicWordPro[topic][docs[i][j]];
				nume_sum+=Math.log(p);
			}
		}
		perplexity=Math.exp(-(nume_sum/deno_sum));
		//System.out.println("perplexity="+Math.exp(-(nume_sum/deno_sum)));
		return perplexity;
	}

	public void inferInitial(){
		for(int i=0;i<M;i++)
		{
			for(int j=0;j<docLength[i];j++)
			{
				int initTopic=(int)(Math.random()*K);
				docWordTopic[i][j]=initTopic;
				docTopicNum[i][initTopic]++;
			}
		}
	}

	public float[][] readTopicWord(String filePath)throws IOException {
		float[][] topicWordPro=new float[K][W];
		InputStreamReader inStrR = new InputStreamReader(new FileInputStream(filePath));
		BufferedReader br = new BufferedReader(inStrR);
		String line = br.readLine();
		String[] topicWord;
		int topicIndex;
		while (line != null) {
			topicWord=line.split(":");
			topicIndex=Integer.parseInt(topicWord[0]);
//			System.out.println(topicWord[1]);
			String[] wordspro=topicWord[1].split(",");
//			System.out.println(wordspro.length);
			for(int i=0;i<wordspro.length;i++,i++)
			{
//				System.out.println(i+":"+wordspro[i]+"\t"+(i+1)+":"+wordspro[i+1]);
				topicWordPro[topicIndex][Integer.parseInt(wordspro[i])]=Float.parseFloat(wordspro[i+1]);
//				topicWordPro[topicIndex][vocab.indexOf(wordspro[i])]=Float.parseFloat(wordspro[i+1]);
			}
			line = br.readLine();
		}
		br.close();
		return topicWordPro;
	}
	
	public String inferGibbsSampling() throws IOException{
		String outInfor="";
		int oldTopic;
		float[] temp=new float[K];
		float sum;
		float total;
		int newTopic=0;
		for(int itr=0;itr<iterNum;itr++)
		{
			for(int i=0;i<M;i++)
			{
				for(int j=0;j<docLength[i];j++)
				{
					oldTopic=docWordTopic[i][j];
					docTopicNum[i][oldTopic]--;
					
					//compute every topic's probability.
					for(int k=0;k<K;k++)
					{
						temp[k]=((docTopicNum[i][k]+alpha)/(docLength[i]-1+K*alpha))*topicWordPro[k][docs[i][j]];
					}
					//normalize
					sum=0;
					for(int k=0;k<K;k++)
					{
						sum+=temp[k];
					}
					total=0;
					for(int k=0;k<K;k++)
					{
						total+=temp[k]/sum;
						temp[k]=total;
					}
					//sampling a new topic
					float random=(float) Math.random();
					for(int k=0;k<K;k++)
					{
						if(temp[k]>random){
							newTopic=k;
							break;
						}
						else continue;
					}
					//change some values of the new topic
					docWordTopic[i][j]=newTopic;
					docTopicNum[i][newTopic]++;
				}
			}
			if(itr==iterNum-2||itr==iterNum-1)
			{
				outInfor+="Inference LDA iteration "+itr+": Perplexity="+perplexity()+"\t";
//				System.out.print("Inference LDA iteration "+itr+": Perplexity="+perplexity());
			}
		}
		return outInfor;
	}

	public void writeDatasetTopicPro(String filePath)throws IOException{
		File file=new File(filePath);
		if (!file.exists()) {
			file.createNewFile();
		}
		BufferedWriter writer=new BufferedWriter(new FileWriter(file));
		float[][] docTopicPro=new float[M][K];
		for(int i=0;i<M;i++)
		{
			float[] topicPro=new float[K];
			float sum=0;
			for(int j=0;j<K;j++)
			{
				sum+=docTopicNum[i][j]+alpha;
			}
			for(int j=0;j<K;j++)
			{
				docTopicPro[i][j]=(docTopicNum[i][j]+alpha)/sum;
			}
		}
		float[] topicPro=new float[K];
		for(int j=0;j<K;j++)
		{
			for(int i=0;i<M;i++)
			{
				topicPro[j]+=docTopicPro[i][j];
			}
			topicPro[j]=topicPro[j]/M;
			writer.write(j+" "+topicPro[j]+" ");
		}
		writer.close();
	}
}
