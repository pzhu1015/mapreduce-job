/**
* @Author pzh
* @Date 2019年9月8日 下午6:09:49
* @Description 
*/
package com.pzh.mapreduce.log;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		args = new String[] {"e:/pzh/test/input/web.log", "e:/pzh/test/outputlog"};
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "log");

		job.setJarByClass(LogDriver.class);
		job.setMapperClass(LogMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setNumReduceTasks(0);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean result = job.waitForCompletion(true);
		System.out.println("=====================Job Finished==================");
		System.exit(result ? 0 : 1);
	}

}
