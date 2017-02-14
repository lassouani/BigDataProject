package bigdata;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class TP3 {
	public enum MyCounters {
		nb_cities,
		nb_pop,
		total_pop
	}

	public static class TP3Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	  public void map(LongWritable keys, Text value, Context context) throws IOException, InterruptedException {
		  if(keys.equals(new LongWritable(0)) && value.toString().contains("Population")) {
			  return;
		  }
		  else {
			  String tokens[] = value.toString().split("\\,", -1);
			  IntWritable result = (tokens[4].equals("")) ? new IntWritable(0) : new IntWritable(1);	
			  
			  if(result.equals(new IntWritable(1))) {
				  context.getCounter(MyCounters.nb_pop).increment(1);  
				  context.getCounter(MyCounters.total_pop).increment(Long.parseLong(tokens[4]));  
			  }
			  context.getCounter(MyCounters.nb_cities).increment(1);  
			  context.write(value, result);
		  }
	  }
  }
  public static class TP3Reducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    public void reduce(Text key, IntWritable value,
            		Context context
                       ) throws IOException, InterruptedException {
    	
    	if(value.equals(new IntWritable(1)))
    		context.write(key, null);
    }
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "TP3");
    job.setNumReduceTasks(1);
    job.setJarByClass(TP3.class);
    job.setMapperClass(TP3Mapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setReducerClass(TP3Reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
