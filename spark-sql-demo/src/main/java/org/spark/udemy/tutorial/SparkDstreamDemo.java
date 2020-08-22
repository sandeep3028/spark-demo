package org.spark.udemy.tutorial;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class SparkDstreamDemo {
	
	public static void main(String[] args) throws InterruptedException {
		
		Logger.getLogger("org.apache").setLevel(Level.ERROR);
		
		SparkConf conf = new SparkConf().setAppName("SparkDstreamDemo").setMaster("local[*]");
		
		JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(1));
		
		JavaReceiverInputDStream<String> logData = sc.socketTextStream("localhost", 8989);
		
		logData.mapToPair(data -> new Tuple2<>(data.split(",")[0], 1))
			.reduceByKeyAndWindow((val1, val2) -> val1 + val2, Durations.seconds(30))
			.print();
		
		sc.start();
		sc.awaitTermination();
	}
	
}
