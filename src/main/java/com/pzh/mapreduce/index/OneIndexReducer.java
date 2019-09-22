/**
* @Author pzh
* @Date 2019年9月10日 下午9:07:10
* @Description 
*/
package com.pzh.mapreduce.index;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OneIndexReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
	
	private LongWritable v = new LongWritable();
	@Override
	protected void reduce(Text k, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		//1. 累加求和
		long sum = 0;
		for (LongWritable value : values) {
			sum += value.get();
		}
		v.set(sum);
		context.write(k, v);
	}
}
