/**
* @Author pzh
* @Date 2019年9月3日 下午9:52:08
* @Description 
*/
package com.pzh.mapreduce.nline;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NLineReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
	private LongWritable v = new LongWritable();
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		long sum = 0;
		for(LongWritable value: values) {
			sum += value.get();
		}
		v.set(sum);
		context.write(key, v);
	}
}
