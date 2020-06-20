package com.daslab.czy.SortAllTopicWords;

import java.io.IOException;

public class MainSort {
	public static void main(String args[]) throws IOException{
		String sourcePath=args[0];
		String writePath=args[1];
		float yita = Float.parseFloat(args[2]);
		
		TopicAllWordSort tawSort=new TopicAllWordSort(sourcePath,writePath, yita);
		tawSort.sorted();
	}
}
