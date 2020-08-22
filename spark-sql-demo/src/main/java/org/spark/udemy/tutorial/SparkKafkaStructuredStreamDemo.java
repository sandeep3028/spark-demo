package org.spark.udemy.tutorial;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class SparkKafkaStructuredStreamDemo {

	public static void main(String[] args) throws StreamingQueryException {
		
		Logger.getLogger("org.apache").setLevel(Level.ERROR);
		
		SparkSession session = SparkSession
									.builder()
									.appName("SparkKafkaStructuredStreamDemo")
									.master("local[*]")
									.getOrCreate();
		
		session.conf().set("spark.sql.shuffle.partitions", "10");
		
		Dataset<Row> data = session.readStream()
								.format("kafka")
								.option("kafka.bootstrap.servers", "localhost:9092")
								.option("subscribe", "viewrecords")
								.load();
		
		data.createOrReplaceTempView("viewfigure");
		
		data = session.sql("select window as window, cast(value as string) as course_name, sum(5) as total_duration from viewfigure group by window(timestamp, '2 minutes'), course_name order by total_duration desc");
		
		
		//trigger can be used for batching, but default micro batch mode is preferred
		data.writeStream()
				.format("console")
				.outputMode(OutputMode.Complete())
				.option("truncate", false)
				.option("numRows", 50)
				.start()
				.awaitTermination();

	}

}
