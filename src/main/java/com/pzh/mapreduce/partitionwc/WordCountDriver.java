/**
* @Author pzh
* @Date 2019年9月1日 下午8:05:20
* @Description 
*/
package com.pzh.mapreduce.partitionwc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String[] paths = new String[] {"e:/pzh/test/input/words.txt", "e:/pzh/test/output2"};
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "wordcount");

		job.setJarByClass(WordCountDriver.class);

		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		//设置切片
//		job.setInputFormatClass(CombineTextInputFormat.class);
//		CombineTextInputFormat.setMaxInputSplitSize(job, 41943040);
		job.setNumReduceTasks(2);
		
		FileInputFormat.setInputPaths(job, new Path(paths[0]));
		FileOutputFormat.setOutputPath(job, new Path(paths[1]));

		boolean result = job.waitForCompletion(true);
		System.out.println("===========================Job Finish========================");
		System.exit(result ? 0 : 1);
	}
}
