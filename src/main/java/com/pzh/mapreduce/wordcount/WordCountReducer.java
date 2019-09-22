/**
* @Author pzh
* @Date 2019年9月1日 下午8:12:10
* @Description 
*/
package com.pzh.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

	private LongWritable v = new LongWritable();
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		long sum = 0;
		for(LongWritable l: values) {
			sum += l.get();
		}
		v.set(sum);
		context.write(key, v);
	}
}
