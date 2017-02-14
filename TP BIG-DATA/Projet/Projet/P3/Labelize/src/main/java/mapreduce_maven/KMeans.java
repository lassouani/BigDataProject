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

		Configuration conf = new Configuration();   // instancer la configuration
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
			job.setNumReduceTasks(1);        // le nombre de reducer
			job.setJarByClass(KMeans.class);  // la class que le jar va utiliser  qui contien le main
			job.setMapperClass(KMeansMapper.class);     //le mapper
			job.setMapOutputKeyClass(Text.class);     // le format de la clé
			job.setMapOutputValueClass(MaxWritable.class);    //formt de la valeur
			job.setReducerClass(KMeansReducer.class);   // la class du reduce
			job.setOutputKeyClass(NullWritable.class);     // le format de la clé du reduer en sortie 
			job.setOutputValueClass(Text.class);    // format dela valeur du reduce
			job.setOutputFormatClass(TextOutputFormat.class);   // format de la sortie final du job 
			job.setInputFormatClass(TextInputFormat.class);   //le format d'entré '
			FileInputFormat.addInputPath(job, new Path(inputFile));
			FileOutputFormat.setOutputPath(job, new Path(output + n + "_folder"));			
			job.waitForCompletion(true);          // attendre que ça fini 
			
			FileSystem fs = FileSystem.get(conf);
			Path source = new Path(output + n + "_folder");
			Path target = new Path(output + n + ".csv");
			FileUtil.copyMerge(fs, source, fs, target, true, conf, null);
		
			

		}

		System.exit(0);

	}

}