package bigdata;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import bigdata.TaggedValue;
import bigdata.TaggedKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Partitioner;


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
	
	public static class TaggedKeyWritable implements WritableComparable<TaggedKeyWritable> {
		public TaggedKey t;

		public TaggedKeyWritable() {
			t = new TaggedKey();
		}
		public TaggedKeyWritable(String name, int type){
			t = new TaggedKey(name, type);
		}
		public void readFields(DataInput in) throws IOException {
			t.keyName = in.readUTF();
			t.typeKey = in.readInt();
		}
		public void write(DataOutput out) throws IOException {
			out.writeUTF(t.keyName);
			out.writeInt(t.typeKey);
		}
		public int compareTo(TaggedKeyWritable o) {

			int result = this.t.keyName.compareTo(o.t.keyName);
			if(result == 0) {
				return -1*(this.t.typeKey.compareTo(o.t.typeKey));
			}
			return result;
			

		}
		
	}	
	
	public static class RSJGrouping extends WritableComparator {
		public RSJGrouping() {
			super(TaggedKeyWritable.class, true);
		}
		public int compare(TaggedKeyWritable w1, TaggedKeyWritable w2) {
			return w1.t.keyName.compareTo(w2.t.keyName);
		}
		
	}
	
	public static class RSJSort extends WritableComparator {
		public RSJSort() {
			super(TaggedKeyWritable.class, true);
		}
		public int compare(TaggedKeyWritable w1, TaggedKeyWritable w2) {
			//return w1.t.typeKey < w2.t.typeKey ? -1 : (w1.t.typeKey==w2.t.typeKey ? 0 : 1);
			return -1*(w1.t.typeKey.compareTo(w2.t.typeKey));
		}
		
	}
	//Mapper pour le fichier worldcitiespop
	public static class RSJMapper1 extends Mapper<LongWritable, Text, TaggedKeyWritable, TaggedValueWritable>{
		public void map(LongWritable keys, Text value, Context context) throws IOException, InterruptedException {
			String tokens[] = value.toString().split("\\,", -1);
			if(keys.equals(new LongWritable(0)) && value.toString().contains("Population")) {
				return;
			}
			if(!tokens[0].equals("") && !tokens[3].equals("")) {
				String mapperKey = tokens[0]+"/"+tokens[3];
				context.write(new TaggedKeyWritable(mapperKey.toLowerCase(), 0), new TaggedValueWritable(tokens[1], 0));
			}
		}

	}
	
	//Mapper pour le fichier region_codes
	public static class RSJMapper2 extends Mapper<LongWritable, Text, TaggedKeyWritable, TaggedValueWritable>{
		public void map(LongWritable keys, Text value, Context context) throws IOException, InterruptedException {
			String tokens[] = value.toString().split("\\,", -1);
			String mapperKey = tokens[0]+"/"+tokens[1];
			context.write(new TaggedKeyWritable(mapperKey.toLowerCase(), 1), new TaggedValueWritable(tokens[2], 1));
		}

	}


	public static class RSJPartitioner extends Partitioner<TaggedKeyWritable, TaggedValueWritable> {

		@Override
		public int getPartition(TaggedKeyWritable key, TaggedValueWritable value, int numPartitions) {
			return (key.t.keyName.hashCode()%numPartitions);
		}
		
	}
  
	
	public static class RSJReducer extends Reducer<TaggedKeyWritable, TaggedValueWritable,NullWritable,Text> {
		String region = null;
	  public void reduce(TaggedKeyWritable key, Iterable<TaggedValueWritable> values, Context context) throws IOException, InterruptedException {
		  
		  for(TaggedValueWritable taggedvalue : values){
			if(taggedvalue.t.typeValue == 1){
				this.region = taggedvalue.t.name;
			}
			else {
				context.write(NullWritable.get(), new Text(taggedvalue.t.name+","+this.region));
			}
		}
		  
	  }
	}
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "TP3");
    
    job.setNumReduceTasks(2);
    job.setJarByClass(TP3.class);
   
    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RSJMapper1.class);
    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RSJMapper2.class);
    
    job.setMapOutputKeyClass(TaggedKeyWritable.class);
    job.setMapOutputValueClass(TaggedValueWritable.class);

    job.setPartitionerClass(RSJPartitioner.class);
    job.setGroupingComparatorClass(RSJGrouping.class);
    job.setSortComparatorClass(RSJSort.class);
    
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
