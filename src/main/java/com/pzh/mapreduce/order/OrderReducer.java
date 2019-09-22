/**
* @Author pzh
* @Date 2019年9月7日 下午5:56:48
* @Description 
*/
package com.pzh.mapreduce.order;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable>{
	@Override
	protected void reduce(OrderBean key, Iterable<NullWritable> values,
			Reducer<OrderBean, NullWritable, OrderBean, NullWritable>.Context context)
			throws IOException, InterruptedException {
		context.write(key, NullWritable.get());
	}
}
