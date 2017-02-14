package bigdata;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class TP3 {
	
	public static class StatsWritable implements Writable {
		public int sum;
		public int count;
		public int max;
		public int min;
		
		public StatsWritable() {
			
		}
		public StatsWritable(int sum, int count, int max, int min) {
			this.sum = sum;
			this.count = count;
			this.max = max;
			this.min = min;
		}
		public void readFields(DataInput in) throws IOException {
			sum = in.readInt();
			count = in.readInt();
			max = in.readInt();
			min = in.readInt();
		}
		public void write(DataOutput out) throws IOException {
			out.writeInt(sum);
			out.writeInt(count);
			out.writeInt(max);
			out.writeInt(min);
		}
	}
	
	
	//public static class TP3Mapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
	public static class TP3Mapper extends Mapper<LongWritable, Text, IntWritable, StatsWritable>{
		int pas;
		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
			this.pas = context.getConfiguration().getInt("pas", 1);
		}
		
		protected int getRangePop(int population) {
			int value = (int)Math.pow(10,(int)Math.floor(Math.log10(population)));
			int rangePas = (int)value/pas;
			int i = 1;
			while((value+i*rangePas)<population)
				i++;
			
			return value+i*rangePas;
			
		}
			
		public void map(LongWritable keys, Text value, Context context) throws IOException, InterruptedException {
			if(keys.equals(new LongWritable(0)) && value.toString().contains("Population")) {
				return;
			}
			else {
			  String tokens[] = value.toString().split("\\,", -1);
			  IntWritable result = (tokens[4].equals("")) ? new IntWritable(0) : new IntWritable(1);	
				  
			  if(result.equals(new IntWritable(1))) {
				int population = Integer.parseInt(tokens[4]);
				//context.write(new IntWritable(this.getRangePop(population)), new IntWritable(1));
				context.write(new IntWritable(this.getRangePop(population)), new StatsWritable(population, 1, population, population)); 
			  }
		
				 
			}
		}
	}
	
	public static class TP3Combiner extends Reducer<IntWritable,StatsWritable,IntWritable, StatsWritable> {
		public void reduce(IntWritable key, Iterable<StatsWritable> values, Context context) throws IOException, InterruptedException {
			int partialSum = 0;
			int partialCount = 0;
			int partialMax = -1;
			int partialMin = -1;
			
			for(StatsWritable i:values) {				
				partialSum += i.sum;
				partialCount += i.count;
				if(partialMax == -1 || i.max > partialMax) 
					partialMax = i.max;
				if(partialMin == -1 || i.min < partialMin)
					partialMin = i.min;
			}
			context.write(key, new StatsWritable(partialSum, partialCount, partialMax, partialMin));
		}
	}
	

  /*Question 1
   * public static class TP3Reducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int tmp = 0;
		for (IntWritable i: values) {
			tmp += i.get();
		}
		context.write(key, new IntWritable(tmp));
    }
  }*/

	public static class TP3Reducer extends Reducer<IntWritable,StatsWritable,Text,Text> {
		protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException {
			context.write(new Text("classe"), new Text("count \t avg \t max \t min"));
		}
	    public void reduce(IntWritable key, Iterable<StatsWritable> values, Context context) throws IOException, InterruptedException {
			int count = 0;
			int sum = 0;
			int max = -1;
			int min = -1;
			
			for (StatsWritable i: values) {
				count += i.count;
				sum += i.sum;
				if(max == -1 || i.max > max)
					max = i.max;
				if(min == -1 || i.min < min)
					min = i.min;
			}
			String output = count+"\t"+(sum/count)+"\t"+max+"\t"+min; 
			context.write(new Text(key.toString()), new Text(output));
	    }
	}
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("pas", args[2]);
    Job job = Job.getInstance(conf, "TP3");
    job.setNumReduceTasks(1);
    job.setJarByClass(TP3.class);
    job.setMapperClass(TP3Mapper.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(StatsWritable.class);
    job.setCombinerClass(TP3Combiner.class);
    job.setReducerClass(TP3Reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(StatsWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
