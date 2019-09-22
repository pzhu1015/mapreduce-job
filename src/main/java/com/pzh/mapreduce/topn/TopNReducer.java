/**
* @Author pzh
* @Date 2019年9月10日 下午9:54:22
* @Description 
*/
package com.pzh.mapreduce.topn;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TopNReducer extends Reducer<FlowBean, Text, Text, FlowBean>{
	
	private TreeMap<FlowBean, Text> flowMap = new TreeMap<>();
	@Override
	protected void reduce(FlowBean key, Iterable<Text> values, Reducer<FlowBean, Text, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		for (Text value : values) {
			FlowBean bean = new FlowBean();
			bean.set(key.getUpFlow(), key.getDownFlow());
			flowMap.put(bean, new Text(value));
			
			if (flowMap.size() > 10) {
				flowMap.remove(flowMap.lastKey());
			}
		}
	}
	
	@Override
	protected void cleanup(Reducer<FlowBean, Text, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Iterator<FlowBean> itr = flowMap.keySet().iterator();
		while(itr.hasNext()) {
			FlowBean v = itr.next();
			context.write(new Text(flowMap.get(v)), v);
		}
	}
}
