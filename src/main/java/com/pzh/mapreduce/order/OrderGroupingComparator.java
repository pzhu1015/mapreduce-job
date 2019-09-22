/**
* @Author pzh
* @Date 2019年9月7日 下午6:19:43
* @Description 
*/
package com.pzh.mapreduce.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingComparator extends WritableComparator{
	protected OrderGroupingComparator() {
		super(OrderBean.class, true);
	}
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		//只要orderId相同，就认为是相同的key
		OrderBean aBean = (OrderBean)a;
		OrderBean bBean = (OrderBean)b;
		int result;
		if (aBean.getOrderId() > bBean.getOrderId()) {
			result = 1;
		}else if (aBean.getOrderId() < bBean.getOrderId()) {
			result = -1;
		}else {
			result = 0;
		}
		return result;
	}
}
