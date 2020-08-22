package org.spark.udemy.tutorial;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;
import shapeless.Tuple;

public class SparkRDDJoin {

	public static void main(String[] args) {
		
Logger.getLogger("org.apache").setLevel(Level.WARN);;
		
		SparkConf sparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]");		
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		
		//Joins on RDD
		  
				List<Tuple2<Integer, Integer>> userIdCount = new ArrayList<>();
				userIdCount.add(new Tuple2<Integer, Integer>(1, 34));
				userIdCount.add(new Tuple2<Integer, Integer>(2, 44));
				userIdCount.add(new Tuple2<Integer, Integer>(3, 24));
				userIdCount.add(new Tuple2<Integer, Integer>(10, 104));
				
				List<Tuple2<Integer, String>> userIdName = new ArrayList<>();
				userIdName.add(new Tuple2<Integer, String>(1, "test1"));
				userIdName.add(new Tuple2<Integer, String>(2, "test2"));
				userIdName.add(new Tuple2<Integer, String>(3, "test3"));
				userIdName.add(new Tuple2<Integer, String>(4, "test4"));
				userIdName.add(new Tuple2<Integer, String>(5, "test5"));
				userIdName.add(new Tuple2<Integer, String>(6, "test6"));
				
				//create PairRDD
				JavaPairRDD<Integer, Integer> userIdCountRDD = sparkContext.parallelizePairs(userIdCount);
				JavaPairRDD<Integer, String> userIdNameRDD = sparkContext.parallelizePairs(userIdName);
				
				//inner join
				System.out.println("\n===inner join===");
				JavaPairRDD<Integer, Tuple2<Integer, String>> innerJoinRDD = userIdCountRDD.join(userIdNameRDD);				
				innerJoinRDD.sortByKey().collect().forEach(System.out::println);
				
				//left outer join
				System.out.println("\n===left join===");
				JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> leftJoinRDD = userIdCountRDD.leftOuterJoin(userIdNameRDD);
				leftJoinRDD.sortByKey().collect().forEach(System.out::println);
				
				leftJoinRDD.collect().forEach(tuple -> {
					System.out.println("ID : "+tuple._1+" Visit Count : "+tuple._2._1+" Name : "+tuple._2._2.orElse("Blank").toUpperCase());
				});
				
				//right outer join
				System.out.println("\n===right join===");
				JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> rightJoinRDD = userIdCountRDD.rightOuterJoin(userIdNameRDD);
				rightJoinRDD.sortByKey().collect().forEach(System.out::println);
				
				rightJoinRDD.foreach(tuple -> {
					System.out.println("ID : "+tuple._1+" Visit Count : "+tuple._2._1.orElse(0)+" Name : "+tuple._2._2.toUpperCase());
				});
				
				//full outer join
				System.out.println("\n===full join===");
				JavaPairRDD<Integer, Tuple2<Optional<Integer>, Optional<String>>> fullJoinRDD = userIdCountRDD.fullOuterJoin(userIdNameRDD);
				fullJoinRDD.sortByKey().collect().forEach(System.out::println);
				fullJoinRDD.foreach(tuple -> {
					System.out.println("ID : "+tuple._1+" Visit Count : "+tuple._2._1.orElse(0)+" Name : "+tuple._2._2.orElse("Blank"));
				});
				
				//Cartesian
				System.out.println("\n===Cartesian cross join===");
				JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> crossJoinRDD =  userIdCountRDD.cartesian(userIdNameRDD);
				crossJoinRDD.collect().forEach(System.out::println);
								
				sparkContext.close();
	}
}
