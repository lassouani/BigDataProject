package mapreduce_maven;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class KMeansResultMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

	public int K;
	public int DIM;
	public String SEPARATOR;
	public Boolean HEADER;
	public MultiDemPoint[] centroids;
	Integer columnPoint[];

	public void setup(Context context) throws IOException {
		//System.out.println("Setup MAPPER EN COURS");
		K = context.getConfiguration().getInt("K", 1);
		DIM = context.getConfiguration().getInt("DIM", 2);
		SEPARATOR = context.getConfiguration().get("SEPARATOR");
		HEADER = context.getConfiguration().getBoolean("HEADER", false);
		columnPoint = new Integer[DIM];
		for(int i=0;i<DIM;i++){
			columnPoint[i] = context.getConfiguration().getInt("PARAM"+i, 0);
		}
		
		//System.out.println("RECUPERATION DES DERNIERS CENTROID");
		centroids = new MultiDemPoint[K];
		URI[] localPaths = context.getCacheFiles();
		FileSystem fs = FileSystem.get(context.getConfiguration());
		BufferedReader data= new BufferedReader(new InputStreamReader(fs.open(new Path(localPaths[0].toString()))));
		
		//On récupère les derniers centroids calculés
		int c = 0;
		for (String line = data.readLine(); line != null; line = data.readLine()) {
			List<Double> coords = new ArrayList<Double>();
			String coord[] = line.toString().split("\\;", -1);
			for(int i=0;i<coord.length;i++){
				coords.add(Double.parseDouble(coord[i]));
			}
			centroids[c] = new MultiDemPoint(coords);
			c++;
		}
		data.close();
		//System.out.println("SETUP MAPPER DONE");
		
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(HEADER && key.equals(new LongWritable(0))) {
			return;
		}
		String tokens[] = value.toString().split("\\"+SEPARATOR, -1);
		List<Double> coordPoint = new ArrayList<Double>();
		for(int i=0;i<DIM;i++) {
			try {
				coordPoint.add(Double.parseDouble(tokens[columnPoint[i]]));
			} catch (NumberFormatException ignore) {
				return;
			}
			
		}
		

		MultiDemPoint p = new MultiDemPoint(coordPoint);

		Integer cPlusProche = 0;
		double distancePlusProche = centroids[cPlusProche].distance(p);
		for(int i=1;i<centroids.length;i++) {
			double distancePoints = centroids[i].distance(p);
			if (distancePlusProche > distancePoints) {
				distancePlusProche = distancePoints;
				cPlusProche = i;
				
			}
		}
		
		String data = value+SEPARATOR+cPlusProche.toString();
		context.write(NullWritable.get(), new Text(data));

		
	}
	


}
