package com.daslab.czy.SortAllTopicWords;

import java.io.*;
import java.util.ArrayList;

public class TopicAllWordSort {
	private String sourcePath;
	private String writePath;
	private float yita;
	
	public TopicAllWordSort(){
		super();
	}
	public TopicAllWordSort(String sourcePath,String writePath, float yita) throws IOException{
		super();
		this.sourcePath=sourcePath;
		this.writePath=writePath;
		this.yita = yita;
	}
	public void sorted() throws IOException{
		File file=new File(writePath+"sortTopicWordAll.txt");
		if (!file.exists()) {
			file.createNewFile();
		}
		BufferedWriter writer=new BufferedWriter(new FileWriter(file));
		
		InputStreamReader inStrR1 = new InputStreamReader(new FileInputStream(sourcePath+"topicWordPro.txt"));
		BufferedReader br1 = new BufferedReader(inStrR1);
		String line1=br1.readLine();
		while(line1!=null){
			String[] topicWordsStrings=line1.split(":");
			writer.write(topicWordsStrings[0]+":");
			String[] wordsPro=topicWordsStrings[1].split(",");
			ArrayList<Float> pro=new ArrayList<>();
			ArrayList<Integer> index=new ArrayList<>();
			for(int i=0;i<wordsPro.length;i++,i++){
				index.add(pro.size());
				pro.add(Float.parseFloat(wordsPro[i+1]));
			}
			for(int i=0;i<pro.size();i++){
				for(int j=i+1;j<pro.size();j++){
					if(pro.get(i)<pro.get(j)){
						float temp=pro.get(i);
						pro.set(i, pro.get(j));
						pro.set(j, temp);
						int t=index.get(i);
						index.set(i, index.get(j));
						index.set(j,t);
					}
				}
			}
			for(int i=0;i<pro.size();i++){
				if(pro.get(i) < yita){
					break;
				}
				writer.write(index.get(i)+" "+pro.get(i)+" ");
			}
			writer.write("\n");
			line1=br1.readLine();
		}
		br1.close();
		writer.close();
	}
}
