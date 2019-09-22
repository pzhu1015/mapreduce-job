/**
* @Author pzh
* @Date 2019年9月8日 下午4:25:25
* @Description 
*/
package com.pzh.mapreduce.table;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {
	@Override
	protected void reduce(Text key, Iterable<TableBean> values,
			Reducer<Text, TableBean, TableBean, NullWritable>.Context context) throws IOException, InterruptedException {
		ArrayList<TableBean> orderBeans = new ArrayList<TableBean>();
		TableBean pdBean = new TableBean();
		for (TableBean value : values) {
			TableBean tmpBean = new TableBean();
			if (value.getFlag().equals("order")) {
				try {
					BeanUtils.copyProperties(tmpBean, value);
				} catch (IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				orderBeans.add(tmpBean);
			}else {
				try {
					BeanUtils.copyProperties(pdBean, value);
				} catch (IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		for (TableBean tableBean : orderBeans) {
			tableBean.setPname(pdBean.getPname());
			context.write(tableBean, NullWritable.get());
		}
	}
}
