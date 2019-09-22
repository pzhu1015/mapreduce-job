/**
* @Author pzh
* @Date 2019年9月8日 下午3:14:34
* @Description 
*/
package com.pzh.mapreduce.outputformat;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FilterReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
	private Text k = new Text();
	@Override
	protected void reduce(Text key, Iterable<NullWritable> values,
			Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
		String line = key.toString();
		line += "\r\n";
		k.set(line);
		for (NullWritable value : values) {
			context.write(k, value);
		}
	}
}
