/**
* @Author pzh
* @Date 2019年9月10日 下午9:54:14
* @Description 
*/
package com.pzh.mapreduce.topn;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopNMapper extends Mapper<LongWritable, Text, FlowBean, Text>{
	
	
	private TreeMap<FlowBean, Text> flowMap = new TreeMap<FlowBean, Text>();
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context)
			throws IOException, InterruptedException {
		FlowBean kBean = new FlowBean();
		Text v = new Text();
		String line = value.toString();
		String[] fields = line.split("\t");
		
		kBean.setUpFlow(Long.parseLong(fields[1]));
		kBean.setDownFlow(Long.parseLong(fields[2]));
		kBean.setSumFlow(Long.parseLong(fields[3]));
		v.set(fields[0]);
		
		flowMap.put(kBean, v);
		if (flowMap.size() > 10) {
			flowMap.remove(flowMap.lastKey());
		}
	}
	
	@Override
	protected void cleanup(Mapper<LongWritable, Text, FlowBean, Text>.Context context)
			throws IOException, InterruptedException {
		Iterator<FlowBean> itr = flowMap.keySet().iterator();
		while(itr.hasNext()) {
			FlowBean k = itr.next();
			context.write(k, flowMap.get(k));
		}
	}
}
