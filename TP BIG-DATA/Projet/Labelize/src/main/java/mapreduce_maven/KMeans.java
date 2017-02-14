package mapreduce_maven;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class KMeans {
	static long startTime = System.nanoTime();

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
		String separator = args[2];
		Integer mesure = Integer.parseInt(args[3]);
		Integer etiquette = Integer.parseInt(args[4]);
		Integer nParam = (args.length - 5);
		Boolean header = false;

		if (args[args.length - 1].equals("true")) {
			nParam--;
			header = true;
		}

		if (nParam < 1) {
			System.out.println("Nombre de paramètres insuffisants");
			System.exit(0);
		}

		Configuration conf = new Configuration();
		conf.setInt("N", nParam);
		conf.set("SEPARATOR", separator);
		conf.setInt("MESURE", mesure);
		conf.setInt("ETIQUETTE", etiquette);
		conf.setBoolean("HEADER", header);
		Integer columnPoint[] = new Integer[nParam];
		for (int i = 5; i < nParam + 5; i++) {
			columnPoint[i - 5] = Integer.parseInt(args[i]);
			conf.setInt("PARAM" + (i - 5), Integer.parseInt(args[i]));
		}

		
		for (int n = nParam-1; n >= 0; n--) {

			System.out.println("\n");
			System.out.println("------------------------------------------------------");
			System.out.println(">> Début de l'étiquetage avec N =" + n);
			System.out.println("------------------------------------------------------\n");

			String inputFile = (n == (nParam-1)) ? input : output + (n+1) + ".csv";
			conf.setInt("STEP", n);
			Job job = Job.getInstance(conf, "KMeans");
			job.setNumReduceTasks(1);
			job.setJarByClass(KMeans.class);
			job.setMapperClass(KMeansMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(MaxWritable.class);
			job.setReducerClass(KMeansReducer.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setInputFormatClass(TextInputFormat.class);
			FileInputFormat.addInputPath(job, new Path(inputFile));
			FileOutputFormat.setOutputPath(job, new Path(output + n + "_folder"));			
			job.waitForCompletion(true);
			
			FileSystem fs = FileSystem.get(conf);
			Path source = new Path(output + n + "_folder");
			Path target = new Path(output + n + ".csv");
			FileUtil.copyMerge(fs, source, fs, target, true, conf, null);
		
			

		}

		System.exit(0);

	}

}