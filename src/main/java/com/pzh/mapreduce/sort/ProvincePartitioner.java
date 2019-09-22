/**
* @Author pzh
* @Date 2019年9月7日 下午4:30:44
* @Description 
*/
package com.pzh.mapreduce.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<FlowBean, Text>{

	@Override
	public int getPartition(FlowBean key, Text value, int numPartitions) {
		String prefix = value.toString().substring(0, 3);
		if ("136".equals(prefix)) {
			return 0;
		}else if ("137".equals(prefix)) {
			return 1;
		}else if ("138".equals(prefix)) {
			return 2;
		}else {
			return 3;
		}
	}

}
