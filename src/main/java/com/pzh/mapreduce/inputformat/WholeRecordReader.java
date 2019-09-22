/**
* @Author pzh
* @Date 2019年9月3日 下午11:02:08
* @Description 
*/
package com.pzh.mapreduce.inputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WholeRecordReader extends RecordReader<Text, BytesWritable> {
	FileSplit split;
	Configuration conf;
	Text key = new Text();
	BytesWritable value = new BytesWritable();
	boolean isProgress = true;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		this.split = (FileSplit) split;
		this.conf = context.getConfiguration();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (isProgress) {
			// 1. 获取fs对象
			Path path = split.getPath();
			FileSystem fs = path.getFileSystem(conf);
			// 2. 获取输入流
			FSDataInputStream fis = fs.open(path);
			// 3. 拷贝
			byte[] buf = new byte[(int) split.getLength()];
			IOUtils.readFully(fis, buf, 0, buf.length);
			// 4. 封装value
			value.set(buf, 0, buf.length);
			// 5. 封装key
			key.set(path.toString());
			// 6. 关闭资源
			IOUtils.closeStream(fis);
			isProgress = false;
			return true;
		}
		return false;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

}
