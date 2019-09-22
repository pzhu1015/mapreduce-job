/**
* @Author pzh
* @Date 2019年9月8日 下午5:35:51
* @Description 
*/
package com.pzh.mapreduce.tablejoin;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistributeCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	
	private HashMap<String, String> pdMap = new HashMap<>();
	private Text k = new Text();
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		URI[] cacheFiles = context.getCacheFiles();
		String path = cacheFiles[0].getPath().toString();
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
		String line;
		while(StringUtils.isNotEmpty(line = reader.readLine())) {
			String[] fields = line.split("\t");
			pdMap.put(fields[0], fields[1]);
		}
		
		IOUtils.closeStream(reader);
	}
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] fields = line.split("\t");
		
		String pname = pdMap.get(fields[1]);
		
		String output = fields[0] + "\t" + pname + "\t" + fields[2];
		k.set(output);
		context.write(k, NullWritable.get());
	}
}
