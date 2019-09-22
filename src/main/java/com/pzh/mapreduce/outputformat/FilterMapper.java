/**
* @Author pzh
* @Date 2019年9月8日 下午3:12:12
* @Description 
*/
package com.pzh.mapreduce.outputformat;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		/*
		 	http://www.baidu.com
			http://www.google.com
			http://cn.bing.com
			http://www.atguigu.com
			http://www.sohu.com
			http://www.sina.com
			http://www.sin2a.com
			http://www.sin2desa.com
			http://www.sindsafa.com
		 */
		context.write(value, NullWritable.get());
	}
}
