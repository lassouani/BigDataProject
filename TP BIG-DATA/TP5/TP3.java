package bigdata;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
	//Country,City,AccentCity,Region,Population,Latitude,Longitude
	public static class CityWritable implements Writable {
		public String country;
		public String city;
		public String accentCity;
		public String region;
		public int population;		
		public double latitude;
		public double longitude;
		
		private int counter;
		private long timestamp;
		public CityWritable() {
			
		}
		public CityWritable(String country, String city, String accentCity, String region, int population, double latitude, double longitude) {
			this.country = country;
			this.city = city;
			this.accentCity = accentCity;
			this.region = region;
			this.population = population;
			this.latitude = latitude;
			this.longitude = longitude;
	
		}
		public void readFields(DataInput in) throws IOException {
			country = in.readUTF();
			city = in.readUTF();
			accentCity = in.readUTF();
			region = in.readUTF();
			population = in.readInt();
			latitude = in.readDouble();
			longitude = in.readDouble();

		}
		public void write(DataOutput out) throws IOException {
			out.writeUTF(country);
			out.writeUTF(city);
			out.writeUTF(accentCity);
			out.writeUTF(region);
			out.writeInt(population);
			out.writeDouble(latitude);
			out.writeDouble(longitude);
			
		}
		
		public int compareTo(CityWritable o) {
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
			return this.country+","+this.city+","+this.region+","+this.population+","+this.latitude+","+this.longitude;
		}
	}	
	

	public static class TopKMapper extends Mapper<LongWritable, Text, NullWritable, CityWritable>{
		public int k;
		private TreeMap<Integer, CityWritable> topKCity = new TreeMap<Integer, CityWritable>();
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			k = conf.getInt("k", 10);
		}
		
		public void map(LongWritable keys, Text value, Context context) {
			//Ignore first line
			if(keys.equals(new LongWritable(0)) && value.toString().contains("Population")) {
				return;
			}
			else {
				String tokens[] = value.toString().split("\\,", -1);
				boolean havePopulation = !(tokens[4].equals(""));	
				if(havePopulation && tokens.length == 7) {
					String country = tokens[0];
					String city = tokens[1];
					String accentCity = tokens[2];
					String region = tokens[3];
					int population = Integer.parseInt(tokens[4]);		
					double latitude = Double.parseDouble(tokens[5]);
					double longitude = Double.parseDouble(tokens[6]);
					topKCity.put(population, new CityWritable(country, city, accentCity, region, population, latitude, longitude));
					if(topKCity.size() > k) {
						topKCity.remove(topKCity.firstKey());
					}
				}
				//context.write(value, result);
		  	}
		}
		public void cleanup(Context context) throws IOException, InterruptedException {
			for(CityWritable cityInfo : topKCity.values()) {
				context.write(NullWritable.get(), cityInfo);
				
			}
		}
  }

  public static class TopKCombiner extends Reducer<NullWritable, CityWritable, NullWritable, CityWritable> {
	  public int k;
	  private TreeMap<Integer, CityWritable> topKCity = new TreeMap<Integer, CityWritable>();
	  public void setup(Context context) throws IOException, InterruptedException {
		  Configuration conf = context.getConfiguration();
		  k = conf.getInt("k", 10);
	  }
	  
	  public void reduce(NullWritable key, Iterable<CityWritable> values, Context context) throws IOException, InterruptedException {
	    	
		  for(CityWritable cityInfo : values)	{
			topKCity.put(cityInfo.population, new CityWritable(cityInfo.country, cityInfo.city, cityInfo.accentCity, cityInfo.region, cityInfo.population, cityInfo.latitude, cityInfo.longitude)); 
			if(topKCity.size() > k) {
				topKCity.remove(topKCity.firstKey());
			}
		  }
		  
		  for(CityWritable cityInfo : topKCity.values()) {			
			context.write(NullWritable.get(), cityInfo);
		  }
	  }
  }
	  
  
  public static class TopKReducer extends Reducer<NullWritable, CityWritable,NullWritable,Text> {
	  public int k;
	  private TreeMap<Integer, String> topKCity = new TreeMap<Integer, String>();
	  public void setup(Context context) throws IOException, InterruptedException {
		  Configuration conf = context.getConfiguration();
		  k = conf.getInt("k", 10);
	  }
	  
	  public void reduce(NullWritable key, Iterable<CityWritable> values, Context context) throws IOException, InterruptedException {
	    	
		  for(CityWritable cityInfo : values){
			topKCity.put(cityInfo.population, cityInfo.toString()); 
			if(topKCity.size() > k) {
				topKCity.remove(topKCity.firstKey());
			}
		  }
		  
		  for(String cityInfo : topKCity.values()) {			
			context.write(NullWritable.get(), new Text(cityInfo));
		  }
    }
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("k", args[2]);
    Job job = Job.getInstance(conf, "TP3");
    
    job.setNumReduceTasks(1);
    job.setJarByClass(TP3.class);
    job.setMapperClass(TopKMapper.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(CityWritable.class);
    job.setCombinerClass(TopKCombiner.class);
    job.setReducerClass(TopKReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    
  }
}
