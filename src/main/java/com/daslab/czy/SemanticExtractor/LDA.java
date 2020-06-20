package com.daslab.czy.SemanticExtractor;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LDA {
	private int K;
	private int iterNum;
	private float alpha;
	private float beta;
	private int[][] docs;
	private int[] docLength;
	private int W;
	private int M;
	private List<String> vocab=new ArrayList<>();
	private int[][] docWordTopic;
	private int[][] docTopicNum;
	private int[][] topicWordNum;
	private int[] topicNum;
	public LDA(){
		super();
	}
	public LDA(int K, int iterNum, float alpha, float beta, int[][] docs, int[] docLength, int W, int M, List<String> vocab){
		this.K=K;
		this.iterNum=iterNum;
		this.alpha=alpha;
		this.beta=beta;
		this.docs=docs;
		this.docLength=docLength;
		this.W=W;
		this.M=M;
		this.vocab=vocab;
		docWordTopic=new int[M][];//store doc-word-topic matrix;
		docTopicNum=new int[M][K];//store the doc-topic-num matrix
		topicWordNum=new int[K][W];//store the topic-word-num matrix
		topicNum=new int[K];//store every topic's words total count
	}
	/*
	 * 给每篇doc中的每个词都随机赋一个值
	 * 并同时更新doc-word-topic matrix; doc-topic-num matrix; topic-word-num matrix; every topic's words total count
	 */
	public void initial(){
		for(int i=0;i<M;i++)
		{
			docWordTopic[i]=new int[docLength[i]];
			for(int j=0;j<docLength[i];j++)
			{
				int initTopic=(int)(Math.random()*K);
				docWordTopic[i][j]=initTopic;
				docTopicNum[i][initTopic]++;
				topicWordNum[initTopic][docs[i][j]]++;
				topicNum[initTopic]++;
			}
		}
	}
	/*
	 * compute the perplexity
	 */
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
				p=(((docTopicNum[i][topic]+alpha)/(docLength[i]+K*alpha))*((topicWordNum[topic][docs[i][j]]+beta)/(topicNum[topic]+W*beta)));
				nume_sum+=Math.log(p);
			}
		}
		perplexity=Math.exp(-(nume_sum/deno_sum));
		return perplexity;
	}
	public String gibbsSampling()
	{
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
					topicWordNum[oldTopic][docs[i][j]]--;
					topicNum[oldTopic]--;
					
					//compute every topic's probability.
					for(int k=0;k<K;k++)
					{
						temp[k]=((docTopicNum[i][k]+alpha)/(docLength[i]-1+K*alpha))*((topicWordNum[k][docs[i][j]]+beta)/(topicNum[k]+W*beta));
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
					topicWordNum[newTopic][docs[i][j]]++;
					topicNum[newTopic]++;
				}
			}
			if(itr==iterNum-2||itr==iterNum-1)
			{
				outInfor+="LDA iteration "+itr+": Perplexity="+perplexity()+"\t";
//				System.out.print("LDA iteration "+itr+": Perplexity="+perplexity());
			}
		}
		return outInfor;
	}
	/*
	 * store every sorted topic
	 */
	public void sortWriteTopicPro(int topK,String filePath) throws IOException{
		File file=new File(filePath);
		if (!file.exists()) {
			file.createNewFile();
		}
		BufferedWriter writer=new BufferedWriter(new FileWriter(file));
		for(int i=0;i<K;i++)
		{
			writer.write(i+":");
			float[] wordPro=new float[W];
			int[] wordIndex=new int[W];
			float sum=0;
			for(int j=0;j<W;j++)
			{
				sum+=topicWordNum[i][j]+beta;
			}
			for(int j=0;j<W;j++)
			{
				wordPro[j]=(topicWordNum[i][j]+beta)/sum;
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
	/*
	 * store every topic
	 */
	public void writeTopicWord(String filePath) throws IOException{
		File file=new File(filePath);
		if (!file.exists()) {
			file.createNewFile();
		}
		BufferedWriter writer=new BufferedWriter(new FileWriter(file));
		
		for(int i=0;i<K;i++)
		{
			writer.write(i+":");
			float sum=0;
			for(int j=0;j<W;j++)
			{
				sum+=(topicWordNum[i][j]+beta);
			}
			for(int j=0;j<W;j++)
			{
//				writer.write(vocab.get(j)+","+((topicWordNum[i][j]+beta)/sum)+",");
				writer.write(j+","+((topicWordNum[i][j]+beta)/sum)+",");
			}
			writer.write("\n");
		}
		writer.close();
	}
	/*
	 * store every dataset-topic-pro matrix
	 */
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