/**
* @Author pzh
* @Date 2019年9月10日 下午9:09:12
* @Description 
*/
package com.pzh.mapreduce.index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OneIndexDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String[] paths = new String[] {"e:/pzh/test/input/reverse_index", "e:/pzh/test/output/reverse_index"};
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "reverse_index");

		job.setJarByClass(OneIndexDriver.class);

		job.setMapperClass(OneIndexMapper.class);
		job.setReducerClass(OneIndexReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.setInputPaths(job, new Path(paths[0]));
		FileOutputFormat.setOutputPath(job, new Path(paths[1]));

		boolean result = job.waitForCompletion(true);
		System.out.println("===========================Job Finish========================");
		System.exit(result ? 0 : 1);
	}

}
