/**
* @Author pzh
* @Date 2019年9月7日 下午5:58:43
* @Description 
*/
package com.pzh.mapreduce.order;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OrderDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		args = new String[] {"e:/pzh/test/input/orders.txt", "e:/pzh/test/output5"};
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "order");

		job.setJarByClass(OrderDriver.class);

		job.setMapperClass(OrderMapper.class);
		job.setReducerClass(OrderReducer.class);

		job.setMapOutputKeyClass(OrderBean.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setGroupingComparatorClass(OrderGroupingComparator.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean result = job.waitForCompletion(true);

		System.out.println("===================Job Finished===================");
		System.exit(result ? 0 : 1);
	}
}
