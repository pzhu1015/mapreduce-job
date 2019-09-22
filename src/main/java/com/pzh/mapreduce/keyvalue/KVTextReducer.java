/**
* @Author pzh
* @Date 2019年9月3日 下午9:23:14
* @Description 
*/
package com.pzh.mapreduce.keyvalue;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KVTextReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
	private LongWritable value = new LongWritable();
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		long sum = 0;
		for (LongWritable l: values) {
			sum += l.get();
		}
		value.set(sum);
		context.write(key, value);
	}
}
