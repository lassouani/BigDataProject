package bigdata;

import org.apache.hadoop.hdfs.util.EnumCounters.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
//import org.apache.commons.lang.StringUtils; 
import org.apache.spark.util.StatCounter;

import scala.Tuple2;

public class TPSpark {

	public static boolean isDouble(String str) {
		try {
			Double.parseDouble(str);
			return true;
		}
		catch(Exception e){
			return false;
		}
	}
	    
	public static void main(String[] args) throws InterruptedException {
		
		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);
		
		
		//Exercice 1
		String inputPath = args[0];
		JavaRDD<String> rdd;
		rdd = context.textFile(inputPath);
		int numPartitions = rdd.getNumPartitions();
		int numExecutors = conf.getInt("spark.executor.instances", 4);
		rdd = rdd.repartition(numExecutors);
		System.out.println("===Exercice 1===");
		System.out.println("Num executors: "+numExecutors);
		System.out.println("Num partitions before repartition: "+numPartitions);
		System.out.println("Num partitions after repartition: "+rdd.getNumPartitions());

		//Exercice 2
		JavaRDD<String[]> rddl = rdd.map(line -> line.split(","));
		JavaRDD<Tuple2<String, Double>> rddc = rddl.map((Function<String[], Tuple2<String, Double>>) data -> {
			
			Double population = -1.;
			if(isDouble(data[4])) {
				population = Double.parseDouble(data[4]);
			}
		
			return new Tuple2<>(data[0], population);
		});
		
		JavaRDD<Tuple2<String, Double>> rddf = rddc.filter((x) -> x._2().compareTo(-1.) != 0);
		System.out.println("===Exercice 2===");
		System.out.println("Nombre de lignes valide:"+rddf.count());
		
		//Exercice 3
		JavaPairRDD<String, Double> rddp = JavaPairRDD.fromJavaRDD(rddf);
		JavaRDD<Double> rddpop = rddp.values();
		JavaDoubleRDD rdds = rddpop.mapToDouble(x -> x); 
		StatCounter stat = rdds.stats();
		System.out.println("===Exercice 3===");
		System.out.println("Min: "+stat.min());
		System.out.println("Max: "+stat.max());
		System.out.println("Sum: "+stat.sum());
		System.out.println("Mean: "+stat.mean());
		System.out.println("Stdev (Standard deviation): "+stat.stdev());
		System.out.println("Variance: "+stat.variance());
		//System.out.println("Liste des populations: "+StringUtils.join(rdds.collect(), ","));
				
		//Exercice 4
		System.out.println("===Exercice 4===");
		JavaPairRDD<Integer, Double> rddId = rdds.keyBy((x) -> (int)Math.floor(Math.log10(x)));
		JavaPairRDD<Integer, StatCounter> rddRbk = rddId.aggregateByKey(new StatCounter(), 
				new Function2<StatCounter, Double, StatCounter>(){	

					private static final long serialVersionUID = 1L;

					public StatCounter call(StatCounter arg0, Double arg1) throws Exception {
						arg0.merge(arg1);
						return arg0;
					}
				},
				new Function2<StatCounter, StatCounter, StatCounter>(){

					private static final long serialVersionUID = 1L;

					public StatCounter call(StatCounter arg0, StatCounter arg1) throws Exception {
						arg0.merge(arg1);
						return arg0;
					}
			 	
				}
		);
		
		System.out.println(rddRbk.sortByKey().collect());
		
		//Exercice 5
		System.out.println("===Exercice 5===");
		String inputPathRegion = args[1];
		JavaRDD<String> rddRegion = context.textFile(inputPathRegion);
		JavaRDD<String[]> rddRl = rddRegion.map(line -> line.split(","));

		JavaRDD<Tuple2<String, String>> rddRc = rddRl.map((Function<String[], Tuple2<String, String>>) data -> {
			String key = data[0]+";"+data[1];
			return new Tuple2<>(key.toLowerCase(), data[2]);
		});
		
		JavaRDD<Tuple2<String, String>> rddCc = rddl.map((Function<String[], Tuple2<String, String>>) data -> {
			String key = data[0]+";"+data[3];
			return new Tuple2<>(key.toLowerCase(), data[2]);
		});
		
		JavaPairRDD<String, String> rddf1 = JavaPairRDD.fromJavaRDD(rddCc);
		JavaPairRDD<String, String> rddf2 = JavaPairRDD.fromJavaRDD(rddRc);
		
		JavaPairRDD<String, Tuple2<String, String>> join = rddf1.join(rddf2);
		join.saveAsTextFile("/join/exo5");

	}
	
}
