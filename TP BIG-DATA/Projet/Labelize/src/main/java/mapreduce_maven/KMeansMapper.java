package mapreduce_maven;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class KMeansMapper extends Mapper<LongWritable, Text, Text, MaxWritable> {

	public Integer N;
	public String SEPARATOR;
	public Boolean HEADER;
	public Integer ETIQUETTE;
	public Integer MESURE;
	public Integer STEP;
	
	Integer columnPoint[];
	private TreeMap<String, MaxWritable> maxMeasure = new TreeMap<String, MaxWritable>();
	public void setup(Context context) throws IOException {
		//System.out.println("Setup MAPPER EN COURS");
		N = context.getConfiguration().getInt("N", 1);
		STEP = context.getConfiguration().getInt("STEP", 0);
		SEPARATOR = context.getConfiguration().get("SEPARATOR");
		
		ETIQUETTE = context.getConfiguration().getInt("ETIQUETTE", 0);
		MESURE = context.getConfiguration().getInt("MESURE", 1);
		if(!STEP.equals(N-1)) {
			ETIQUETTE = 0;
			MESURE = 1;
		}
		
		HEADER = context.getConfiguration().getBoolean("HEADER", false);
		columnPoint = new Integer[N];
		for(int i=0;i<N;i++){
			if(STEP.equals(N-1))
				columnPoint[i] = context.getConfiguration().getInt("PARAM"+i, 0);
			else
				columnPoint[i] = 3+i;
		}
		
		
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(HEADER && key.equals(new LongWritable(0))) {
			return;
		}
		
		String tokens[] = value.toString().split("\\"+SEPARATOR, -1);
		
		double measure;
		try {
			measure = Double.parseDouble(tokens[MESURE]);
		} catch (NumberFormatException ignore) {;
			 return;
		}
		

		
		List<String> keyStep = new ArrayList<String>();
		for(int i=0;i<=STEP;i++) {
			keyStep.add(tokens[columnPoint[i]]);
		}
		
		String keyMap = String.join(",", keyStep);
		
		MaxWritable data = new MaxWritable(measure, tokens[ETIQUETTE]);
		
		if(!maxMeasure.containsKey(keyMap) || (maxMeasure.get(keyMap).value < measure)) {
			maxMeasure.put(keyMap, data);
		}
		

		
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException {
		for(Entry<String, MaxWritable> entry : maxMeasure.entrySet()) {
			  String key = entry.getKey();
			  MaxWritable value = entry.getValue();
			  context.write(new Text(key), value);
			 
		}
		

	}

}
