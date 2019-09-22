/**
* @Author pzh
* @Date 2019年9月3日 下午10:59:49
* @Description 
*/
package com.pzh.mapreduce.inputformat;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class WholeFileInputFormat extends FileInputFormat<Text, BytesWritable>{
	@Override
	public org.apache.hadoop.mapreduce.RecordReader<Text, BytesWritable> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
		
		WholeRecordReader reader = new WholeRecordReader();
		reader.initialize(split, context);
		return reader;
	}
}
