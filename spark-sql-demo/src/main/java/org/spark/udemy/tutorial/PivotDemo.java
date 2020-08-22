package org.spark.udemy.tutorial;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class PivotDemo {

	public static void main(String[] args) {
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession sparkSession = SparkSession
										.builder()
										.appName("PivotDemo")
										.master("local[*]")
										.getOrCreate();
		
		/*
		 *********** reading the csv
		 */
		
		  Dataset<Row> inputData = sparkSession.read()
				.option("header", true)
				.option("inferSchema", true)
				.csv("C:\\Users\\sande\\Downloads\\original\\Practicals\\Starting Workspace\\Project\\src\\main\\resources\\exams\\students.csv");
		
		  
		  inputData = inputData.groupBy("subject")
				  .pivot("year")
				  .agg( round(avg(col("score")),2).alias("average"),
						round(stddev(col("score")),2).alias("stddev") );
		  
		  inputData.show();
		  
		  sparkSession.close();
	}

}
