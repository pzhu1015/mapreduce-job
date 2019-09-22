/**
* @Author pzh
* @Date 2019年8月31日 下午9:37:09
* @Description 
*/
package com.pzh.mapreduce.flowsum;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
	private Text k = new Text();
	private FlowBean v = new FlowBean();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		//1. 获取一行
		String line = value.toString();
		//2. 切割\t
		String[] fields = line.split("\t");
		//3. 封装对象
		k.set(fields[1]);
		long upFlow = Long.parseLong(fields[4]);
		long downFlow = Long.parseLong(fields[5]);
		v.setUpFlow(upFlow);
		v.setDownFlow(downFlow);
		//4. 输出
		context.write(k, v);
	}
}
