/**
* @Author pzh
* @Date 2019年9月3日 下午9:54:50
* @Description 
*/
package com.pzh.mapreduce.nline;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NLineDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		args = new String[] { "e:/pzh/test/input/phone_data.txt", "e:/pzh/test/output5" };
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "nline");

		NLineInputFormat.setNumLinesPerSplit(job, 8);
		job.setInputFormatClass(NLineInputFormat.class);

		job.setJarByClass(NLineDriver.class);

		job.setMapperClass(NLineMapper.class);
		job.setReducerClass(NLineReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean result = job.waitForCompletion(true);
		System.out.println("================Job Finished=======================");
		System.exit(result ? 0 : 1);

	}

}
