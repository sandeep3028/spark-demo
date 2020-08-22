package org.spark.udemy.tutorial;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

public class SparkKafkaDSteamDemo {

	public static void main(String[] args) throws InterruptedException {
		
		Logger.getLogger("org.apache").setLevel(Level.ERROR);
		
		SparkConf conf = new SparkConf().setAppName("spark-kafka-dstream").setMaster("local[*]");
		
		//batch is 1 second long
		JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(1));
		
		Collection<String> topics = Arrays.asList("viewrecords");
		
		Map<String, Object> params = new HashMap<String, Object>();

		params.put("bootstrap.servers", "localhost:9092");
		params.put("key.deserializer", StringDeserializer.class);
		params.put("value.deserializer", StringDeserializer.class);
		params.put("group.id", "spark-kafka-dstreamgrp");
		params.put("auto.offset.reset", "latest");
		
		JavaInputDStream<ConsumerRecord<Object, Object>> data = KafkaUtils.createDirectStream(sc, LocationStrategies.PreferConsistent(),
					ConsumerStrategies.Subscribe(topics, params));
		
		/*
		 * Window and Slide Interval is mentioned below while doing reduceByKey
		 * Window : Aggregation will work on all DStream in that duration
		 * Slide Interval : Duration after which the DStream will be outputed
		 */
		data.mapToPair(val -> new Tuple2<>(val.value(), 1L))
			.reduceByKeyAndWindow((val1, val2) -> val1 + val2, Durations.seconds(30),Durations.seconds(35))
			.mapToPair(val -> val.swap())
			.transformToPair(rdd -> rdd.sortByKey(false))
			.print(100);
		
		sc.start();
		sc.awaitTermination();
		
		
	}
}
