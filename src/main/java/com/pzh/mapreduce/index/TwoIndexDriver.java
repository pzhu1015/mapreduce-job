/**
* @Author pzh
* @Date 2019年9月10日 下午9:09:12
* @Description 
*/
package com.pzh.mapreduce.index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TwoIndexDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String[] paths = new String[] {"e:/pzh/test/output/reverse_index/part-r-00000", "e:/pzh/test/output/reverse_index_two"};
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "reverse_index_two");

		job.setJarByClass(TwoIndexDriver.class);

		job.setMapperClass(TwoIndexMapper.class);
		job.setReducerClass(TwoIndexReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(paths[0]));
		FileOutputFormat.setOutputPath(job, new Path(paths[1]));

		boolean result = job.waitForCompletion(true);
		System.out.println("===========================Job Finish========================");
		System.exit(result ? 0 : 1);
	}

}
