package bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import bigdata.TP3.Point2DWritable;

public class RandomPointInputFormat extends InputFormat<IntWritable,Point2DWritable>{

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
		int splitSize = context.getConfiguration().getInt("splitSize", 30);
		List<InputSplit> splitList = new ArrayList<InputSplit>();
		for(int i=0;i<splitSize;i++){
			splitList.add(i, new FakeInputSplit());
		}
		return splitList;

	}

	@Override
	public RecordReader<IntWritable, Point2DWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new RandomPointReader();
	}

}
