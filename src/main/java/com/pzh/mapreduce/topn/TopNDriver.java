/**
* @Author pzh
* @Date 2019年9月10日 下午9:42:38
* @Description 
*/
package com.pzh.mapreduce.topn;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopNDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		args = new String[] { "e:/pzh/test/output/flowsum/part-r-00000", "e:/pzh/test/output/topn" };
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "topn");

		job.setJarByClass(TopNDriver.class);

		job.setMapperClass(TopNMapper.class);
		job.setReducerClass(TopNReducer.class);

		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean result = job.waitForCompletion(true);
		System.out.println("==================Job Finished=================");
		System.exit(result ? 0 : 1);
	}

}
