/**
* @Author pzh
* @Date 2019年9月1日 下午8:06:09
* @Description 
*/
package com.pzh.mapreduce.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
	private Text k = new Text();
	private LongWritable v = new LongWritable(1);
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
//		System.out.println("偏移量: " + key.get());
		StringTokenizer token = new StringTokenizer(value.toString());
		while(token.hasMoreTokens()) {
			k.set(token.nextToken());
			context.write(k, v);
		}
	}
}
