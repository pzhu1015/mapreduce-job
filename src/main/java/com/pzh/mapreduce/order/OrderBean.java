/**
* @Author pzh
* @Date 2019年9月7日 下午5:39:45
* @Description 
*/
package com.pzh.mapreduce.order;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class OrderBean implements WritableComparable<OrderBean>{

	private int orderId;
	private double price;
	
	public int getOrderId() {
		return orderId;
	}

	public void setOrderId(int orderId) {
		this.orderId = orderId;
	}

	public double getPrice() {
		return price;
	}

	public void setPrice(double price) {
		this.price = price;
	}

	public OrderBean() {
		super();
		// TODO Auto-generated constructor stub
	}

	public OrderBean(int orderId, double price) {
		super();
		this.orderId = orderId;
		this.price = price;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(orderId);
		out.writeDouble(price);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		orderId = in.readInt();
		price = in.readDouble();
	}

	@Override
	public int compareTo(OrderBean o) {
		//先按orderId排序，如果相同按价格降序排序
		if (orderId > o.getOrderId()) {
			return 1;
		}else if (orderId < o.getOrderId()) {
			return -1;
		}else {
			if (price > o.getPrice()) {
				return -1;
			}else if (price < o.getPrice()) {
				return 1;
			}else {
				return 0;
			}
		}
	}

	@Override
	public String toString() {
		return orderId + "\t" + price;
	}

}
