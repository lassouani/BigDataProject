package mapreduce_maven;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansReducer extends Reducer<IntWritable,CentroidWritable,NullWritable,Text> {
	public static CentroidWritable merge(Iterable<CentroidWritable> values) {
		CentroidWritable resume = null;
		Iterator<CentroidWritable> it= values.iterator();
		if (it.hasNext())  {
			resume = it.next().clone();
			while(it.hasNext()) resume.merge(it.next());
		}
		return resume;
	}
	
	public void reduce(IntWritable key, Iterable<CentroidWritable> values,Context context) throws IOException, InterruptedException {
		CentroidWritable resume = merge(values);
		context.write(NullWritable.get(), new Text(resume.toString().replaceAll("[\r\n]+", "")));
	}
}
