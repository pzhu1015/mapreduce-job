/**
* @Author pzh
* @Date 2019年9月8日 下午4:07:45
* @Description 
*/
package com.pzh.mapreduce.table;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TableBean implements Writable  {

	private String id;
	private String pid;
	private int amount;
	private String pname;
	private String flag;
	
	
	public TableBean() {
		super();
		// TODO Auto-generated constructor stub
	}
	
	

	public TableBean(String id, String pid, int amount, String pname, String flag) {
		super();
		this.id = id;
		this.pid = pid;
		this.amount = amount;
		this.pname = pname;
		this.flag = flag;
	}



	public String getId() {
		return id;
	}



	public void setId(String id) {
		this.id = id;
	}



	public String getPid() {
		return pid;
	}



	public void setPid(String pid) {
		this.pid = pid;
	}



	public int getAmount() {
		return amount;
	}



	public void setAmount(int amount) {
		this.amount = amount;
	}



	public String getPname() {
		return pname;
	}



	public void setPname(String pname) {
		this.pname = pname;
	}



	public String getFlag() {
		return flag;
	}



	public void setFlag(String flag) {
		this.flag = flag;
	}



	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(id);
		out.writeUTF(pid);
		out.writeInt(amount);
		out.writeUTF(pname);
		out.writeUTF(flag);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id = in.readUTF();
		pid = in.readUTF();
		amount = in.readInt();
		pname = in.readUTF();
		flag = in.readUTF();
	}



	@Override
	public String toString() {
		return id + "\t" + amount + "\t" + pname;
	}

}
