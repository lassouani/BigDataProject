package mapreduce_maven;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class KMeans {
	static long startTime = System.nanoTime();
	static String basePath = "/centroids/";
	public static MultiDemPoint[] getCentroids(Configuration conf, String input, String separator, int dimension,
			Integer[] columnPoint, int k) throws IOException {

		// Création des premiers centroids
		MultiDemPoint[] centroids = new MultiDemPoint[k];
		FileSystem fs = FileSystem.get(conf);
		BufferedReader lines = new BufferedReader(new InputStreamReader(fs.open(new Path(input)), "UTF-8"));
		String line;
		String data[];
		Boolean lineIsCorrect;
		int i = 0;
		do {

			line = lines.readLine();
			if(line == null) {
				break;
			}
			data = line.toString().split("\\" + separator, -1);
			List<Double> coordCentroid = new ArrayList<Double>();
			lineIsCorrect = true;

			for (int j = 0; (j < dimension && lineIsCorrect); j++) {
				try {
					coordCentroid.add(Double.parseDouble(data[columnPoint[j]]));
					
				} catch (NumberFormatException ignore) {
					lineIsCorrect = false;
				}
			}

			if (lineIsCorrect) {
				Boolean add = true;
				for(int j=0;j<i;j++) {
					if(centroids[j].getCoords().equals(coordCentroid)){
						add = false;
						break;
					}
				}
				
				if(add) {
					centroids[i] = new MultiDemPoint(coordCentroid);
					i++;
				}
			}
		} while (i < k);
		fs.close();

		return centroids;

	}
	
	public static MultiDemPoint[] getNewCentroids(Configuration conf, String input, int dimension, int k) throws IOException {

		// Création des premiers centroids
		MultiDemPoint[] centroids = new MultiDemPoint[k];
		FileSystem fs = FileSystem.get(conf);
		BufferedReader lines = new BufferedReader(new InputStreamReader(fs.open(new Path(input)), "UTF-8"));
		String line;
		String data[];
		int i = 0;
		do {

			line = lines.readLine();
			if(line == null) {
				break;
			}
			data = line.toString().split("\\;", -1);
			List<Double> coordCentroid = new ArrayList<Double>();
			for (int j = 0;j < dimension; j++) {				
				coordCentroid.add(Double.parseDouble(data[j]));
			}
			centroids[i] = new MultiDemPoint(coordCentroid);
			i++;
			
		} while (i < k);
		fs.close();

		return centroids;

	}

	public static void saveCentroids(MultiDemPoint[] centroids, long startTime, int numIteration) {
		String file = startTime + "_centroids_" + numIteration+".dat";
		try {
			Path pt = new Path(basePath + file);
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));
			// TO append data to a file, use fs.append(Path f)
			for(MultiDemPoint point : centroids){
				br.write(point.toString());
				br.write("\r\n"); 
			}
		
			br.close();
		} catch (Exception e) {
			System.out.println("File not found");
		}

	}
	


	public static void main(String[] args) throws Exception {

		// Vérification qu'il y ait assez de paramètres
		if (args.length < 5) {
			System.out.println("Nombre de paramètres incorrects");
			System.exit(0);
		}

		/*
		 * PARAMETERS IN OUT K SEPARATOR COLUMNS ... [HEADER]
		 */

		// Définition des paramètres du job
		String input = args[0];
		String output = args[1];
		Integer k = Integer.parseInt(args[2]);
		String separator = args[3];
		Integer dimension = (args.length - 4);
		Boolean header = false;

		if (args[args.length-1].equals("true")) {
			dimension--;
			header = true;
		}

		if (dimension < 1) {
			System.out.println("Nombre de paramètres insuffisants");
			System.exit(0);
		}


		Configuration conf = new Configuration();
		conf.setInt("K", k);
		conf.set("SEPARATOR", separator);
		conf.setInt("DIM", dimension);
		conf.setBoolean("HEADER", header);
		Integer columnPoint[] = new Integer[dimension];
		for (int i = 4; i < dimension+4; i++) {
			columnPoint[i - 4] = Integer.parseInt(args[i]);
			conf.setInt("PARAM"+(i-4), Integer.parseInt(args[i]));
		}
		
		int numIteration = 0;
		boolean converged = false;
		String inputCentroids = input;

		MultiDemPoint[] centroids = new MultiDemPoint[k];
		centroids = getCentroids(conf, inputCentroids, separator, dimension, columnPoint, k);
		saveCentroids(centroids, startTime, numIteration);
		
		while (!converged) {

			String nextCentroids = basePath+ startTime + "_centroids_" + (numIteration+1);
			
			Job job = Job.getInstance(conf, "KMeans");
			

			job.addCacheFile(new Path(basePath+startTime + "_centroids_" + numIteration+".dat").toUri());
			job.setNumReduceTasks(1);
			job.setJarByClass(KMeans.class);
			job.setMapperClass(KMeansMapper.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(CentroidWritable.class);
			job.setReducerClass(KMeansReducer.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setInputFormatClass(TextInputFormat.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(nextCentroids+"_folder"));
			
			job.waitForCompletion(true);
			
			FileSystem fs = FileSystem.get(conf);
			Path source = new Path(nextCentroids+"_folder");
			Path target = new Path(nextCentroids+".dat");
			FileUtil.copyMerge(fs, source, fs, target, true, conf, null);
			System.out.println("\n");
			System.out.println("-------------------------------");
			System.out.println(">> Nouveaux centroids calculés");
			System.out.println("-------------------------------\n");
	
			MultiDemPoint[] newCentroids = new MultiDemPoint[k]; 
			newCentroids = getNewCentroids(conf, nextCentroids+".dat", dimension, k);
			for(MultiDemPoint p : newCentroids){
				System.out.println(p.toString());
			}
			System.out.println("\n");
			converged = converge(centroids, newCentroids);

			numIteration++;
			centroids = newCentroids;
			
		}
		
		
		
		// On créer le fichier final avec le numéro du cluster
		
		Job job = Job.getInstance(conf, "KMeans");
		
		System.out.println("\n");
		System.out.println("------------------------------------------------------");
		System.out.println(">> Concaténation du fichier avec le numéro du cluster");
		System.out.println("------------------------------------------------------\n");

		job.addCacheFile(new Path(basePath+startTime + "_centroids_" + numIteration+".dat").toUri());		
		job.setNumReduceTasks(1);
		job.setJarByClass(KMeans.class);
		job.setMapperClass(KMeansResultMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		System.exit(job.waitForCompletion(true)? 0 : 1);
		

	}

	private static boolean converge(MultiDemPoint[] centroids, MultiDemPoint[] newCentroids) {
		for(int i=0;i<centroids.length;i++){
			if(!centroids[i].converge(newCentroids[i])) {
				return false;
			}
		}
		return true;
	}

}