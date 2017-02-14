package bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import bigdata.TP3.Point2DWritable;

public class RandomPointReader extends RecordReader<IntWritable, Point2DWritable>{
	protected int pointNumber;
	protected int countPoints;
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.pointNumber = 0;
		this.countPoints = context.getConfiguration().getInt("countPoints", 10);
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return (this.countPoints > this.pointNumber);
	}

	@Override
	public IntWritable getCurrentKey() throws IOException, InterruptedException {
		this.pointNumber++;
		return new IntWritable(this.pointNumber-1);
	}

	
	public double generateDouble(){
		double lower = 0;
		double upper = 1;
		return Math.random() * (upper - lower) + lower;
	}
	
	@Override
	public Point2DWritable getCurrentValue() throws IOException, InterruptedException {	
		return new Point2DWritable(this.generateDouble(), this.generateDouble());
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
