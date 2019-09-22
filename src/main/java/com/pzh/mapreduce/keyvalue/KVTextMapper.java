/**
* @Author pzh
* @Date 2019年9月3日 下午9:19:30
* @Description 
*/
package com.pzh.mapreduce.keyvalue;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KVTextMapper extends Mapper<Text, Text, Text, LongWritable>{
	private LongWritable v = new LongWritable(1);
	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		context.write(key, v);
	}
}
