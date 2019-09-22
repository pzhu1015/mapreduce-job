/**
* @Author pzh
* @Date 2019年9月10日 下午9:18:26
* @Description 
*/
package com.pzh.mapreduce.index;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TwoIndexReducer extends Reducer<Text, Text, Text, Text> {
	
	private Text v = new Text();
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		StringBuffer sb = new StringBuffer();
		for (Text value : values) {
			sb.append(value.toString().replace("\t", "-->") + "\t");
		}
		v.set(sb.toString());
		context.write(key, v);
	}
}
