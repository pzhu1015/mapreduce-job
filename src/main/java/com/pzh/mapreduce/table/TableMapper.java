/**
* @Author pzh
* @Date 2019年9月8日 下午4:15:53
* @Description 
*/
package com.pzh.mapreduce.table;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean>{
	private String name;
	private Text k = new Text();
	private TableBean v = new TableBean();
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, TableBean>.Context context)
			throws IOException, InterruptedException {
		FileSplit inputSplit  = (FileSplit)context.getInputSplit();
		name = inputSplit.getPath().getName();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TableBean>.Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split("\t");
		if (name.startsWith("order")){
			v.setId(fields[0]);
			v.setPid(fields[1]);
			v.setAmount(Integer.parseInt(fields[2]));
			v.setPname("");
			v.setFlag("order");
			
			k.set(fields[1]);
			
		}else {
			v.setId("");
			v.setPid(fields[0]);
			v.setAmount(0);
			v.setPname(fields[1]);
			v.setFlag("pd");
			
			k.set(fields[0]);
		}
		context.write(k, v);
	}
}
