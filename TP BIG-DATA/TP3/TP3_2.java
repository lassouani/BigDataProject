import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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

	static enum UpdateCount {
		nb_cities, nb_pop, total_pop

	}

	

	public static class TP3Mapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		int base;
		public void setup(Context context){
			Configuration conf = context.getConfiguration();
			String param = conf.get("base");
			base=Integer.parseInt(param);
		}	
		
		public int logarithm (int val , int base){
			return (int) Math.round(Math.log(val)/Math.log(base));
		}
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			int pop_log;

			if (key.equals(new LongWritable(0)))
				return;

			else {
				String ligne[] = value.toString().split(",", -1);
			
				if (!ligne[4].equals("")){
					pop_log = Integer.parseInt(ligne[4]);
					pop_log = logarithm(pop_log, base);
					context.write(new Text(String.valueOf((int) Math.pow(base, pop_log))),new IntWritable(Integer.parseInt(ligne[4])));
				
				}
			
		}
		}

	}

	public static class TP3Reducer extends
			Reducer<Text, IntWritable, Text, Text> {
		
		public void setup(Context context) throws IOException, InterruptedException{
			context.write(null, new Text("Class\tCount\tAvg\tMax\tMin") );
		}
		
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int count = 0;
			int sum = 0;
			int min = Integer.MAX_VALUE;
			int max = -1;
			
			
			for (IntWritable i : values){
				count++;
				sum+=i.get();
				min=i.get() < min ? i.get():min;
				max=i.get()>max ? i.get(): max;
			
			}
			context.write(key, new Text(count+" "  +sum/count+' '+max+' '+min));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("base", args[2]);
		Job job = Job.getInstance(conf, "TP3");
		job.setNumReduceTasks(1);
		job.setJarByClass(TP3.class);
		job.setMapperClass(TP3Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(TP3Reducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		Path path = new Path(args[1]);
		FileSystem hdfs = FileSystem.get(conf);
		
		if ( hdfs.exists( path )) 
		{
			hdfs.delete( path, true );
		} 
		
		
		FileOutputFormat.setOutputPath(job, path);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		job.getCounters().findCounter(UpdateCount.nb_cities).getValue();
		job.getCounters().findCounter(UpdateCount.nb_pop).getValue();
		job.getCounters().findCounter(UpdateCount.total_pop).getValue();

	}
}
