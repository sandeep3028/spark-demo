package org.spark.udemy.tutorial;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class BoringWord {
	
	public static void main(String[] args) {
		
		SparkConf sparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]");		
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
			
		JavaRDD<String> fileData = sparkContext.textFile("C:\\Sandeep\\Spark-demo\\input-spring.txt");
		JavaRDD<String> boring = sparkContext.textFile("C:\\Sandeep\\Spark-demo\\boringwords.txt");
		
		System.out.println("=========================================");
		
		List<String> boringWordList  = boring.flatMap(x -> Arrays.asList(x).iterator()).collect();

		
		JavaRDD<String> lowercaseRdd = fileData.map(data -> data.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());
		
		JavaRDD<String> trimRdd = lowercaseRdd.filter(data -> data.trim().matches("[^\\n]+"));
			
		JavaRDD<String> flatRdd = trimRdd.flatMap(data -> Arrays.asList(data.split(" ")).iterator());
		
		JavaRDD<String> filterBoringRdd = flatRdd.filter(data -> !boringWordList.contains(data));
		
		JavaPairRDD<String, Long> pairRdd = filterBoringRdd.mapToPair(data -> new Tuple2<>(data, 1L));
		
		JavaPairRDD<String, Long> reduceRdd  = pairRdd.reduceByKey((value1, value2) -> value1+value2);
		
		JavaPairRDD<Long, String> sortedRdd  = reduceRdd.mapToPair(tuple -> new Tuple2<>(tuple._2 , tuple._1)).sortByKey(false);
		
		sortedRdd.take(50).forEach(System.out::println);
		
		//sortedRdd.repartition(1).saveAsTextFile("C:\\Sandeep\\Spark-demo\\keywords.txt");
		
/*		fileData
			.map(data -> data.replaceAll("[^a-zA-Z\\s]", "").toLowerCase())
			.filter(data -> data.trim().matches("[^\\n]+"))
			.flatMap(data -> Arrays.asList(data.split(" ")).iterator())
			.filter(data -> !boringWordList.contains(data))
			.mapToPair(data -> new Tuple2<>(data, 1L))
			.reduceByKey((value1, value2) -> value1+value2)
			.mapToPair(tuple -> new Tuple2<>(tuple._2 , tuple._1))
			.sortByKey(false)
			.take(10).forEach(System.out::println);*/
						
		Scanner scanner = new Scanner(System.in);
		scanner.nextLine();
		
		sparkContext.close();
		
	}
	
}
