/**
* @Author pzh
* @Date 2019年9月8日 下午4:38:00
* @Description 
*/
package com.pzh.mapreduce.table;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TableDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		args = new String[] {"e:/pzh/test/input/join", "e:/pzh/test/outputjoin"};
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "table join");

		job.setJarByClass(TableDriver.class);
		job.setMapperClass(TableMapper.class);
		job.setReducerClass(TableReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TableBean.class);
		job.setOutputKeyClass(TableBean.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean result = job.waitForCompletion(true);
		System.out.println("=====================Job Finished==================");
		System.exit(result ? 0 : 1);
	}

}
