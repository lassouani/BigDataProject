import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TP5 {

	
	public static class TP5Mapper extends
	
			Mapper<LongWritable, Point2DWritable, LongWritable, Point2DWritable> {
		public void map(LongWritable key, Point2DWritable value, Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class TP5Reducer extends
			Reducer<LongWritable, Point2DWritable, Text, Text> {
		public void reduce(LongWritable key, Iterable<Point2DWritable> values,
				Context context) throws IOException, InterruptedException {
			for (Point2DWritable val : values) {

//						context.write(new Text(key.toString()), new Point2DWritable(val.p2d.x, val.p2d.y));
				context.write(new Text(key.toString()), new Text (new Point2DWritable(val.x, val.y).toString()));
			}

			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "TP5");
		job.setNumReduceTasks(1);
		job.setJarByClass(TP5.class);
		
		job.setMapperClass(TP5Mapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Point2DWritable.class);
		
		job.setReducerClass(TP5Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(RandomPointInputFormat.class);
		System.out.println("input format class"+job.getInputFormatClass());
		
		
		
		int nbPointsPerSplit = Integer.parseInt(args[1]);
		int nbMapper = Integer.parseInt(args[2]);
		
		conf.setInt("nbPointsPerSplit", nbPointsPerSplit);
		conf.setInt("nbMapper", nbMapper);


		//FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
