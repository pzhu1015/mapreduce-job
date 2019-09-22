/**
* @Author pzh
* @Date 2019年9月6日 下午10:28:13
* @Description 
*/
package com.pzh.mapreduce.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowCountSortDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		args = new String[] { "e:/pzh/test/output4", "e:/pzh/test/output5" };
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "count sort");

		job.setJarByClass(FlowCountSortDriver.class);

		job.setMapperClass(FlowCountSortMapper.class);
		job.setReducerClass(FlowCountSortReducer.class);

		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		job.setPartitionerClass(ProvincePartitioner.class);
		job.setNumReduceTasks(4);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean result = job.waitForCompletion(true);
		System.out.println("==================Job Finished=================");
		System.exit(result ? 0 : 1);
	}

}
