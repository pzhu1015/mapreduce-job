/**
* @Author pzh
* @Date 2019年8月31日 下午10:08:48
* @Description 
*/
package com.pzh.mapreduce.flowsum;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean>{

	private FlowBean v = new FlowBean();
	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Context context)
			throws IOException, InterruptedException {
		long sum_upFlow = 0;
		long sum_downFlow = 0;
		//1. 累加求和
		for (FlowBean f: values) {
			sum_upFlow += f.getUpFlow();
			sum_downFlow += f.getDownFlow();
		}
		v.set(sum_upFlow, sum_downFlow);
		//2. 输出
		context.write(key, v);
	}
}
