/**
* @Author pzh
* @Date 2019年9月3日 下午9:25:53
* @Description 
*/
package com.pzh.mapreduce.keyvalue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KVTextDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		args = new String[] { "e:/pzh/test/input/words.txt", "e:/pzh/test/output4" };
		Configuration conf = new Configuration();
		conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
		Job job = Job.getInstance(conf, "keyvlaue");

		job.setJarByClass(KVTextDriver.class);

		job.setMapperClass(KVTextMapper.class);
		job.setReducerClass(KVTextReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean result = job.waitForCompletion(true);
		System.out.println("====================Job Finished==============");
		System.exit(result ? 0 : 1);

	}
}
