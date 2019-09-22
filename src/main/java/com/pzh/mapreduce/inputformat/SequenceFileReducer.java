/**
* @Author pzh
* @Date 2019年9月3日 下午11:25:23
* @Description 
*/
package com.pzh.mapreduce.inputformat;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SequenceFileReducer extends Reducer<Text, BytesWritable, Text, BytesWritable>{
	@Override
	protected void reduce(Text key, Iterable<BytesWritable> values,
			Reducer<Text, BytesWritable, Text, BytesWritable>.Context context) throws IOException, InterruptedException {
		for (BytesWritable value: values) {
			context.write(key, value);
		}
	}
}
