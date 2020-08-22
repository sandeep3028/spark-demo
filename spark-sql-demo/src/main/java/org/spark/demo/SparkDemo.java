package org.spark.demo;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

public class SparkDemo {
	
	
	public static void main(String[] args) throws InterruptedException {
		SparkSession sparkSession = SparkSession
				.builder()
				.appName("SprkDemo")
				.master("local[3]")
				.getOrCreate();
		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
		
		SQLContext sqlContext = new SQLContext(sparkSession);
		
	    Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		Dataset<Row> dataset = sqlContext.read().json("C:\\Sandeep\\Spark-demo\\employee.json");
		
		dataset.show();
		dataset.printSchema();
		dataset.select("name").show();
		dataset.select(dataset.col("name"), dataset.col("age").plus(1)).show();
		dataset.filter(dataset.col("age").gt(21)).show();
		dataset.groupBy("age").count().show();
		
		
		
		List<Integer> intList = Arrays.asList(1,2,3,4,5,6,7,8,9,10,11,12,13,14,14);
		JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(intList, 3);
		javaRDD.foreach((s)->
		{
			System.out.println(s);
		});
		
	   JavaStreamingContext javaStreamingContext = new JavaStreamingContext(javaSparkContext, Durations.seconds(1));
	   Map<String, Object> kafkaParams = new HashMap<>();
	   kafkaParams.put("bootstrap.servers", "localhost:9092");
	   kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
	   kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
	   kafkaParams.put("group.id", "g1");
	   kafkaParams.put("auto.offset.reset", "latest");
	   kafkaParams.put("enable.auto.commit", false);
	   List<String> topics = Arrays.asList("spark-test");
	   
	   JavaInputDStream<ConsumerRecord<String, String>> messages = 
			   KafkaUtils.createDirectStream(
					   javaStreamingContext, 
			     LocationStrategies.PreferConsistent(), 
			     ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams));
	   
	   messages.foreachRDD(rdd-> {
		   rdd.foreach(record->{
			   String value = record.value();
			   String key = record.key();
			   System.out.println(key+" : "+value);
		   });
		   
	   });
	   
	   javaStreamingContext.start();
	   javaStreamingContext.awaitTermination();
		
		Thread.sleep(100000);
		
		javaSparkContext.stop();
		javaSparkContext.close();
	}
}
