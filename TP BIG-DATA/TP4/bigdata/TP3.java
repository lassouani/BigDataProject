package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class TP3 {
	
	public static class Point2DWritable implements Writable {
		public double x;
		public double y;
		
		private int counter;
		private long timestamp;
		public Point2DWritable() {
			
		}
		public Point2DWritable(double x, double y) {
			this.x = x;
			this.y = y;
	
		}
		public void readFields(DataInput in) throws IOException {
			x = in.readDouble();
			y = in.readDouble();

		}
		public void write(DataOutput out) throws IOException {
			out.writeDouble(x);
			out.writeDouble(y);
		}
		
		public int compareTo(Point2DWritable o) {
			int thisValue = this.counter;
			int thatValue = o.counter;
			return (thisValue < thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
		}
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = result * prime + counter;
			result = prime*result + (int) (timestamp ^(timestamp >>> 32));
			return result;
		}
		public String toString() {
			return this.x+" "+this.y;
		}
	}	

  public static class TP3Mapper
       extends Mapper<IntWritable,Point2DWritable,IntWritable,Point2DWritable>{
	  public void map(IntWritable key, Point2DWritable value, Context context
			  ) throws IOException, InterruptedException {
		  context.write(key,value);
	  }
  }
  public static class TP3Reducer extends Reducer<IntWritable,Point2DWritable,Text,Text> {
	public void reduce(IntWritable key, Iterable<Point2DWritable> values, Context context) throws IOException, InterruptedException {
    	
    	for(Point2DWritable i:values) {				
    		context.write(new Text(key.toString()), new Text(i.toString()));
    	}
      
    }
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("splitSize", args[1]);
    conf.set("countPoints", args[2]);
    Job job = Job.getInstance(conf, "TP3");
    job.setNumReduceTasks(1);
    job.setJarByClass(TP3.class);
    job.setMapperClass(TP3Mapper.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Point2DWritable.class);
    job.setReducerClass(TP3Reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setInputFormatClass(RandomPointInputFormat.class);
    //FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[0]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
