/**
* @Author pzh
* @Date 2019年9月7日 下午5:52:13
* @Description 
*/
package com.pzh.mapreduce.order;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable>{
	private OrderBean k = new OrderBean();
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, OrderBean, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split("\t");
		k.setOrderId(Integer.parseInt(fields[0]));
		k.setPrice(Double.parseDouble(fields[2]));
		context.write(k, NullWritable.get());
	}
}
