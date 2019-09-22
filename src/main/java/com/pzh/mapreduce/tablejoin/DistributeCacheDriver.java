/**
* @Author pzh
* @Date 2019年9月8日 下午5:32:06
* @Description 
*/
package com.pzh.mapreduce.tablejoin;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DistributeCacheDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		args = new String[] {"e:/pzh/test/input/join/order.txt", "e:/pzh/test/outputcachejoin"};
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "table join");

		job.setJarByClass(DistributeCacheDriver.class);
		job.setMapperClass(DistributeCacheMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.addCacheFile(new URI("file:///e:/pzh/test/input/join/pd.txt"));
		job.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean result = job.waitForCompletion(true);
		System.out.println("=====================Job Finished==================");
		System.exit(result ? 0 : 1);
	}

}
