/**
* @Author pzh
* @Date 2019年9月8日 下午3:19:43
* @Description 
*/
package com.pzh.mapreduce.outputformat;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class FRecordWriter extends RecordWriter<Text, NullWritable> {

	private FSDataOutputStream fosatguigu;
	private FSDataOutputStream fosother;
	public FRecordWriter(TaskAttemptContext job) {
		//1. 获取文件系统
		try {
			FileSystem fs = FileSystem.get(job.getConfiguration());
			fosatguigu = fs.create(new Path("e:/pzh/test/output/atguigu.log"));
			fosother = fs.create(new Path("e:/pzh/test/output/other.log"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//2. 创建输出到atguigu.log的输出流
		//3. 创建输出到other.log的输出流
	}
	@Override
	public void write(Text key, NullWritable value) throws IOException, InterruptedException {
		if (key.toString().contains("atguigu")) {
			fosatguigu.write(key.toString().getBytes());
		}else {
			fosother.write(key.toString().getBytes());
		}
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		IOUtils.closeStream(fosatguigu);
		IOUtils.closeStream(fosother);
	}

}
