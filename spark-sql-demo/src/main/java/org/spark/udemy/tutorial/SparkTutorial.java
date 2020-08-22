package org.spark.udemy.tutorial;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.spark_project.dmg.pmml.Array;

import avro.shaded.com.google.common.collect.Iterables;
import scala.Tuple2;
import shapeless.Tuple;

public class SparkTutorial {

	public static void main(String[] args) {
		
		List<Integer> inputData = new ArrayList<>();
		inputData.add(10);
		inputData.add(20);
		inputData.add(30);
		inputData.add(40);
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);;
		
		SparkConf sparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]");		
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		
		JavaRDD<Integer> rdd = sparkContext.parallelize(inputData);
		
		//reduce
		Integer result = rdd.reduce((value1, value2) -> value1 + value2);
		
		//mapping
		JavaRDD<Double> sqrtRdd = rdd.map(value -> Math.sqrt(value));
		
		//sqrtRdd.foreach(value -> System.out.println(value));
		//sqrtRdd.foreach(System.out::println);
		
		//This function will be sent to multiple CPU, so it should be serializable which is not the case. My Laptop is multiple CPU
		sqrtRdd.collect().forEach(System.out::println);
				
		System.out.println("Total : "+result);
		
		JavaRDD<Long> countRDD = sqrtRdd.map(value -> 1L);
		
		Long count = countRDD.reduce((value1, value2) -> value1 + value2);
		
		System.out.println("Count : "+count);
		
		//Using tuples
		JavaRDD<Tuple2<Integer, Double>> sqrtTupleRdd = rdd.map(value -> new Tuple2<>(value, Math.sqrt(value)));
		
		
		//PariRRD
		List<String> inputLog = new ArrayList<>();
		inputLog.add("Warn:Log1 15 DEC");
		inputLog.add("Warn:Log1 8 JAN");
		inputLog.add("Error:Log1 7 FEB");
		inputLog.add("Error:Log1 4 JAN");
		inputLog.add("Info:Log1 5 APR");
		inputLog.add("Info:Log1 12 DEC");
		
		/*JavaRDD<String> rawLogRDD = sparkContext.parallelize(inputLog);		
		JavaPairRDD<String, String> pairLogRDD = rawLogRDD.mapToPair(log -> {
			String[] logArr = log.split(":");
			return new Tuple2<>(logArr[0], logArr[1]);
		});
				
		JavaPairRDD<String, Long> countRdd = pairLogRDD.mapToPair(tuple -> new Tuple2<>(tuple._1, 1L));
		
		JavaPairRDD<String, Long> logTypeCount = countRdd.reduceByKey((val1, val2) -> val1 + val2);		
		logTypeCount.foreach(tuple -> System.out.println(tuple._1+" "+tuple._2));
		This code is cleaned below by using Fluent API
		*/
		
		sparkContext.parallelize(inputLog)
			.mapToPair(log -> new Tuple2<>(log.split(":")[0], 1L))
			.reduceByKey((val1, val2) -> val1 + val2)
			.foreach(tuple -> System.out.println(tuple._1+" "+tuple._2));
		
		//groupBy, although this is not suggested performance wise		
		sparkContext.parallelize(inputLog)
			.mapToPair(log -> new Tuple2<>(log.split(":")[0], 1L))
			.groupByKey()
			.foreach(tuple -> System.out.println(tuple._1+" "+Iterables.size(tuple._2)));
	
		sparkContext.parallelize(inputLog)
			.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
			.filter(value -> value.length()> 1)
			.collect()
			.forEach(System.out::println);
		
		
		JavaRDD<String> fileData = sparkContext.textFile("C:\\Sandeep\\Spark-demo\\input-spring.txt");
		JavaRDD<String> boring = sparkContext.textFile("C:\\Sandeep\\Spark-demo\\boringwords.txt");
		
		System.out.println("=========================================");
		
		List<String> boringWordList  = boring.flatMap(x -> Arrays.asList(x).iterator()).collect();
		
		/*fileData
		.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
		.filter(value -> value.length()> 1)
		.collect()
		.forEach(System.out::println);*/
		
		
		  fileData
			.map(data -> data.replaceAll("[^a-zA-Z\\s]", "").toLowerCase())
			.filter(data -> data.trim().matches("[^\\n]+"))
			.flatMap(data -> Arrays.asList(data.split(" ")).iterator())
			.filter(data -> !boringWordList.contains(data))
			.mapToPair(data -> new Tuple2<>(data, 1L))
			.reduceByKey((value1, value2) -> value1+value2)
			.mapToPair(tuple -> new Tuple2<>(tuple._2 , tuple._1))
			.sortByKey(false)
			.take(10).forEach(System.out::println);
	
		
		/*fileData
		.flatMap(input -> Arrays.asList(input.split(" ")).iterator())
		.filter(word -> !boringWordList.contains(word))
		.mapToPair(word -> new Tuple2<>(word, 1L))
		.reduceByKey((val1, val2) -> val1 + val2)
		.filter(tuple -> tuple._2 > 10)
		.foreach(tuple -> System.out.println("Key : "+ tuple._1+" Value : "+tuple._2()));*/
								
		sparkContext.close();
		
	}
	
}
