/**
* @Author pzh
* @Date 2019年9月3日 下午9:46:26
* @Description 
*/
package com.pzh.mapreduce.nline;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NLineMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	private Text k = new Text();
	private LongWritable v = new LongWritable(1);
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		StringTokenizer token = new StringTokenizer(value.toString());
		while(token.hasMoreTokens()) {
			k.set(token.nextToken());
			context.write(k, v);
		}
	}
}
