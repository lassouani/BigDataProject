package bigdata;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import bigdata.TaggedValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;


public class TP3 {
	//Country,City,AccentCity,Region,Population,Latitude,Longitude
	public static class TaggedValueWritable implements Writable {
		public TaggedValue t;

		public TaggedValueWritable() {
			t = new TaggedValue();
		}
		public TaggedValueWritable(String name, int type){
			t = new TaggedValue(name, type);
		}
		public void readFields(DataInput in) throws IOException {
			t.name = in.readUTF();
			t.typeValue = in.readInt();
		}
		public void write(DataOutput out) throws IOException {
			out.writeUTF(t.name);
			out.writeInt(t.typeValue);
		}
		
	}	
	
	//Mapper pour le fichier worldcitiespop
	public static class RSJMapper1 extends Mapper<LongWritable, Text, Text, TaggedValueWritable>{
		public void map(LongWritable keys, Text value, Context context) throws IOException, InterruptedException {
			String tokens[] = value.toString().split("\\,", -1);
			if(keys.equals(new LongWritable(0)) && value.toString().contains("Population")) {
				return;
			}
			String mapperKey = tokens[0]+"/"+tokens[3];
			context.write(new Text(mapperKey.toLowerCase()), new TaggedValueWritable(tokens[1], 0));
		}

	}
	
	//Mapper pour le fichier region_codes
	public static class RSJMapper2 extends Mapper<LongWritable, Text, Text, TaggedValueWritable>{
		public void map(LongWritable keys, Text value, Context context) throws IOException, InterruptedException {
			String tokens[] = value.toString().split("\\,", -1);
			String mapperKey = tokens[0]+"/"+tokens[1];
			context.write(new Text(mapperKey.toLowerCase()), new TaggedValueWritable(tokens[2], 1));
		}

	}


	  
  
	public static class RSJReducer extends Reducer<Text, TaggedValueWritable,NullWritable,Text> {
 
	  public void reduce(Text key, Iterable<TaggedValueWritable> values, Context context) throws IOException, InterruptedException {
		  List<String> waitCity = new ArrayList<String>();
		  String region = null;
		  for(TaggedValueWritable tagged : values){
			if(tagged.t.typeValue == 1){
				region = tagged.t.name;
				for(String cityToWrite: waitCity) {
					context.write(NullWritable.get(), new Text(cityToWrite+","+region));
				}
			}
			else {
				if(region == null) {
					waitCity.add(tagged.t.name);
				}
				else {
					context.write(NullWritable.get(), new Text(tagged.t.name+","+region));
				}
			}
			
		  }
		  
	  }
	}
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "TP3");
    
    job.setNumReduceTasks(1);
    job.setJarByClass(TP3.class);
   
    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RSJMapper1.class);
    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RSJMapper2.class);
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(TaggedValueWritable.class);

    job.setReducerClass(RSJReducer.class);
    job.setOutputKeyClass(TaggedValueWritable.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    //job.setInputFormatClass(MultipleInputsFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
    
  }
}
