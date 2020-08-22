package org.spark.udemy.tutorial;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkSQLDemo {

	public static void main(String[] args) {
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession sparkSession = SparkSession
										.builder()
										.appName("SparkSQLDemo")
										.master("local[*]")
										.getOrCreate();
		
		
		 //********** reading the csv
		 
		
		  Dataset<Row> dataSet = sparkSession.read()
									.option("header", true)
									.csv("src/main/resources/students.csv");
			
		
		 //********** Basic operations like show, count and first on dataset
		 
		
		dataSet.show();
		
		Long numOfRows = dataSet.count();
		System.out.println("Number of rows : "+numOfRows);
		
		String subject = dataSet.first().getAs("subject");		
		System.out.println("First row subject : "+subject);
		
		
		
		/*
		 *********** filters on dataset
		 */
		
		/*		 
		//filters using expression
		//Dataset<Row> mathDataSet = dataSet.filter("subject = 'Math' and year >= 2007");
		
		//filters using lambdas
		//Dataset<Row> mathDataSet = dataSet.filter(row -> row.getAs("subject").equals("Math"));

		
		//filters using java Colums
		//Dataset<Row> mathDataSet = dataSet.filter(col("subject").equalTo("Math")
		//								.and(col("year").geq(2007)));
		 */
		
		/*
		 *********** Full SQL Syntax
		 */
		
		/*
		//dataSet.createOrReplaceTempView("students");
		//Dataset<Row> mathDataSet = sparkSession.sql("select student_id, subject, score, grade from students");
		
		//Dataset<Row> mathDataSet = sparkSession.sql("select max(score) from students");
		
		//mathDataSet.show();
		*/
		
		/*
		 ***********  Making small test data using list
		 */
				
		/*List<Row> inMemory = new ArrayList<Row>();
		inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
		inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
		inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
		inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
		inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));
		
		StructField[] structFields = new StructField[] {
			new StructField("level", DataTypes.StringType, false, Metadata.empty()),
			new StructField("datetime", DataTypes.StringType, false, Metadata.empty()),
		};
		
		StructType schema = new StructType(structFields);		
		Dataset<Row> inMemoryDataSet = sparkSession.createDataFrame(inMemory, schema);
		inMemoryDataSet.createOrReplaceTempView("logdata");
		

		//Dataset<Row> testDataSet = sparkSession.sql("select level, count(datetime) from logdata group by level order by level");
		
		//if you need the list of group by 
		//testDataSet = sparkSession.sql("select level, collect_list(datetime) from logdata group by level order by level");
		
		//Multiple column group by
		//Dataset<Row> testDataSet = sparkSession.sql("select level, date_format(datetime, 'MMMM') as month, count(1) from logdata group by level, month");
		*/
		
		
		/*
		 *********** multiple group by and order by to sort month and level
		 */
		
		/*Dataset<Row> dataSet = sparkSession.read()
									.option("header", true)
									.csv("C:\\Users\\sande\\Downloads\\original\\Practicals\\Extras for Module 2\\biglog.txt");
*/
		/*dataSet.createOrReplaceTempView("logdata");
		
		Dataset<Row> testDataSet = sparkSession.sql("select level, date_format(datetime, 'MMMM') as month, "
				+ "count(1) as total "
				+ "from logdata group by level, month order by cast(first(date_format(datetime, 'M')) as int), level");		
		testDataSet.show(100);*/
				
		
		/*
		 *********** total of all the log count
		 */
		/*
		 * testDataSet.createOrReplaceTempView("resulttable");
		//Dataset<Row> totaldataset = sparkSession.sql("select sum(total) from resulttable");
		//totaldataset.show();
		 */
		
		/*
		 * multiple group by and order by to sort month and level using Java DataFrame API
		 */
		
		/*Dataset<Row> dataFrame = dataSet.select(col("level"), 
												date_format(col("datetime"),"MMMM").alias("month"),
												date_format(col("datetime"),"M").alias("monthnum").cast(DataTypes.IntegerType))
										 .groupBy(col("level"),col("month"),col("monthnum"))
										 .count()
										 .orderBy(col("monthnum"),col("level"))
										 .drop(col("monthnum"));*/
		
		/*
		 * Pivot table
		 */
		
		/*Object[] monthArr = new Object[] {"January","February","March","April","May","June","July","August","September","October","November","December"};
		
		Dataset<Row> dataFrame = dataSet.select(col("level"), 
				date_format(col("datetime"),"MMMM").alias("month"))
				.groupBy(col("level")).pivot(col("month"),Arrays.asList(monthArr)).count();*/
		
		/*
		 * agg function
		 */
		
		/*Dataset<Row> dataFrame = sparkSession.read()
				.option("header", true)
				.csv("C:\\Users\\sande\\Downloads\\original\\Practicals\\Starting Workspace\\Project\\src\\main\\resources\\exams\\students.csv");
		
		dataFrame = dataFrame.groupBy(col("subject")).agg(max(col("score")).cast(DataTypes.IntegerType).alias("max"), 
														  min(col("score")).cast(DataTypes.IntegerType).alias("min"));*/
		
		/*
		 * udf
		 */
		
/*		Dataset<Row> dataFrame = sparkSession.read()
				.option("header", true)
				.csv("C:\\Users\\sande\\Downloads\\original\\Practicals\\Starting Workspace\\Project\\src\\main\\resources\\exams\\students.csv");
		
		
		//sparkSession.udf().register("hasPassed",grade -> grade.equals("A+"),DataTypes.BooleanType);
		
		sparkSession.udf().register("hasPassed",grade -> {
			if(grade.equals("A+")) {
				return "Excellent";
			}else if(grade.equals("A")) {
				return "Good";
			}else if(grade.equals("B") || grade.equals("C")){
				return "Pass";
			}else {
				return "Fail";
			}
		},DataTypes.StringType);
		
		dataFrame = dataFrame.withColumn("result", callUDF("hasPassed", col("grade")));
		
		
		dataFrame.show(100);*/
				
		sparkSession.close();
		
	}
}
