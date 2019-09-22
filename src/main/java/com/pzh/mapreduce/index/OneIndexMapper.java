/**
* @Author pzh
* @Date 2019年9月10日 下午9:02:12
* @Description 
*/
package com.pzh.mapreduce.index;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class OneIndexMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	private String name;
	private Text k = new Text();
	private LongWritable v = new LongWritable(1);

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		FileSplit inputSplit = (FileSplit) context.getInputSplit();
		name = inputSplit.getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] fields = line.split(" ");

		for (String word : fields) {
			k.set(word + "--" + name);
			context.write(k, v);
		}
	}
}
