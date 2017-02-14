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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class KMeans {
	static long startTime = System.nanoTime();


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
			if (line == null) {
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
				for (int j = 0; j < i; j++) {
					if (centroids[j].getCoords().equals(coordCentroid)) {
						add = false;
						break;
					}
				}

				if (add) {
					centroids[i] = new MultiDemPoint(coordCentroid);
					i++;
				}
			}
		} while (i < k);
		fs.close();

		return centroids;

	}
	


	public static MultiDemPoint[] getNewCentroids(Configuration conf, String input, int dimension, int k)
			throws IOException {

		// Création des premiers centroids
		MultiDemPoint[] centroids = new MultiDemPoint[k];
		FileSystem fs = FileSystem.get(conf);
		BufferedReader lines = new BufferedReader(new InputStreamReader(fs.open(new Path(input)), "UTF-8"));
		String line;
		String data[];
		int i = 0;
		do {

			line = lines.readLine();
			if (line == null) {
				break;
			}
			data = line.toString().split("\\;", -1);
			List<Double> coordCentroid = new ArrayList<Double>();
			for (int j = 0; j < dimension; j++) {
				coordCentroid.add(Double.parseDouble(data[j]));
			}
			centroids[i] = new MultiDemPoint(coordCentroid);
			i++;

		} while (i < k);
		fs.close();

		return centroids;

	}

	public static void saveCentroids(MultiDemPoint[] centroids, long startTime, int numIteration, String file) {

		try {
			Path pt = new Path(file);
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));
			// TO append data to a file, use fs.append(Path f)
			for (MultiDemPoint point : centroids) {	
				if(point != null) {
					br.write(point.toString());
					br.write("\r\n");
				}
				
			}

			br.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Problem with centroid -> check saveCentroids function");
		}

	}

	public static void main(String[] args) throws Exception {

		// Vérification qu'il y ait assez de paramètres
		if (args.length < 6) {
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
		Integer nParam = Integer.parseInt(args[4]);
		Integer dimension = (args.length - 5);
		Boolean header = false;

		if (args[args.length - 1].equals("true")) {
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
		for (int i = 5; i < dimension + 5; i++) {
			columnPoint[i - 5] = Integer.parseInt(args[i]);
			conf.setInt("PARAM" + (i - 5), Integer.parseInt(args[i]));
		}

		
		for (int n = 0; n < nParam; n++) {
			for (int a = 0; (a < Math.pow(k, n) && n>0) || (n == 0 && a ==0 ); a++) {
				
				System.out.println("\n");
				System.out.println("------------------------------------------------------");
				System.out.println(">> Début de KMEANS avec N ="+n+" et K="+a);
				System.out.println("------------------------------------------------------\n");
				
				
				String inputCentroids = (n == 0) ? input : "/tmp/output_"+startTime+"/"+(n-1)+"/"+a+".dat";
				FileSystem fileSystem = FileSystem.get(conf);
				if(!fileSystem.exists(new Path(inputCentroids))) {
					continue;
				}

				int numIteration = 0;
				boolean converged = false;

				MultiDemPoint[] centroids = new MultiDemPoint[k];
				centroids = getCentroids(conf, inputCentroids, separator, dimension, columnPoint, k);
				String file = "/tmp/centroids_"+startTime + "/" + numIteration +"_"+ n +"_"+ a + ".dat";
				saveCentroids(centroids, startTime, numIteration, file);

				while (!converged) {

					String nextCentroids = "/tmp/centroids_"+startTime + "/"  + (numIteration + 1)+"_"+n+"_"+a;

					Job job = Job.getInstance(conf, "KMeans");

					job.addCacheFile(new Path("/tmp/centroids_"+startTime + "/"  + numIteration +"_"+ n +"_"+ a + ".dat").toUri());
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
					FileInputFormat.addInputPath(job, new Path(inputCentroids));
					FileOutputFormat.setOutputPath(job, new Path(nextCentroids + "_folder"));

					job.waitForCompletion(true);

					FileSystem fs = FileSystem.get(conf);
					Path source = new Path(nextCentroids + "_folder");
					Path target = new Path(nextCentroids + ".dat");
					FileUtil.copyMerge(fs, source, fs, target, true, conf, null);
					System.out.println("\n");
					System.out.println("-------------------------------");
					System.out.println(">> Nouveaux centroids calculés");
					System.out.println("-------------------------------\n");

					MultiDemPoint[] newCentroids = new MultiDemPoint[k];
					newCentroids = getNewCentroids(conf, nextCentroids + ".dat", dimension, k);
					for (MultiDemPoint p : newCentroids) {
						if(p != null) {
							System.out.println(p.toString());
						}
					}
					System.out.println("\n");
					converged = converge(centroids, newCentroids);

					numIteration++;
					centroids = newCentroids;

				}

				// On créer le fichier final avec le numéro du cluster
				conf.setInt("FileK", a);
				conf.setInt("FileN", n);
				Job job = Job.getInstance(conf, "KMeans");
				
				System.out.println("\n");
				System.out.println("------------------------------------------------------");
				System.out.println(">> Concaténation du fichier avec le numéro du cluster");
				System.out.println("------------------------------------------------------\n");

				job.addCacheFile(new Path("/tmp/centroids_"+startTime + "/"  + numIteration +"_"+ n +"_"+ a  + ".dat").toUri());
				job.setNumReduceTasks(1);
				job.setJarByClass(KMeans.class);
				job.setMapperClass(KMeansResultMapper.class);
				job.setMapOutputKeyClass(NullWritable.class);
				job.setMapOutputValueClass(Text.class);
				job.setOutputFormatClass(TextOutputFormat.class);
				job.setInputFormatClass(TextInputFormat.class);
				FileInputFormat.addInputPath(job, new Path(inputCentroids));
				FileOutputFormat.setOutputPath(job, new Path("/tmp/"+startTime+"/"+n));
				//LazyOutputFormat.setOutputFormatClass(job,TextOutputFormat.class);
				for(int b=0;b<Math.pow(k, n+1)*k;b++){
					MultipleOutputs.addNamedOutput(job, "file"+b, TextOutputFormat.class, NullWritable.class, Text.class);
				}
				job.waitForCompletion(true);
				
				//Integer.toString(i, base);
				FileSystem fs = FileSystem.get(conf);
				for(int b=0;b<k;b++) {
					Path source = new Path("/tmp/"+startTime +"/"+n+"/"+(a*k+b)+"/");
					Path target = new Path("/tmp/output_"+startTime+"/"+n+"/"+(a*k+b)+".dat");
					if(fs.exists(source)) {
						FileUtil.copyMerge(fs, source, fs, target, true, conf, null);						
					}
					
					//fs.createNewFile(target);
					
				}
				String delFolder = "/tmp/"+startTime +"/"+n+"/";
				fs.delete(new Path(delFolder), true);
				
			}

		}
		
		FileSystem fs = FileSystem.get(conf);
		Path source = new Path("/tmp/output_"+startTime+"/"+(nParam-1));
		Path target = new Path(output);
		FileUtil.copyMerge(fs, source, fs, target, true, conf, null);
		fs.delete(new Path("/tmp/"+startTime +"/"), true);
		fs.delete(new Path("/tmp/centroids_"+startTime +"/"), true);
		System.exit(0);

	}

	private static boolean converge(MultiDemPoint[] centroids, MultiDemPoint[] newCentroids) {
		for (int i = 0; i < centroids.length; i++) {
			if (centroids[i]!= null && !centroids[i].converge(newCentroids[i])) {
				return false;
			}
		}
		return true;
	}

}