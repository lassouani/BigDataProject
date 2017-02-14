package mapreduce_maven;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansReducer extends Reducer<Text,MaxWritable,NullWritable,Text> {
	
	
	public void reduce(Text key, Iterable<MaxWritable> values,Context context) throws IOException, InterruptedException {
		MaxWritable nextMax;
		Iterator<MaxWritable> it= values.iterator();
		MaxWritable result = it.next();
		while (it.hasNext())  {
			nextMax = it.next();
			if(nextMax.value > result.value)
				result = nextMax;
		}
		String data = result.name+","+result.value+","+key;
		context.write(NullWritable.get(), new Text(data));
	}
}
