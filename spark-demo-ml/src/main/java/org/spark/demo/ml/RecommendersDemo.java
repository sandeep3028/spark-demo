package org.spark.demo.ml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import java.util.List;

public class RecommendersDemo {
	
	public static void main(String[] args) {
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession session = SparkSession
									.builder()
									.appName("RecommendersDemo")
									.master("local[*]")
									.getOrCreate();
		
		Dataset<Row> inputData = session
				.read()
				.option("inferSchema", true)
				.option("header", true)
				.csv("src/main/resources/VPPcourseViews.csv");
				
		inputData = inputData.withColumn("proportionWatched", col("proportionWatched").multiply(100));
		
		//inputData = inputData.groupBy("userId").pivot("courseId").sum("proportionWatched");
		
//		Dataset<Row>[] trainAndHoldoutData = inputData.randomSplit(new double[] {0.8, 0.2});
//		Dataset<Row> trainData = trainAndHoldoutData[0];
//		Dataset<Row> holdoutData = trainAndHoldoutData[1];
		
		ALS als = new ALS()
				.setMaxIter(10)
				.setRegParam(0.1)
				.setUserCol("userId")
				.setItemCol("courseId")
				.setRatingCol("proportionWatched");
		
		
		ALSModel model = als.fit(inputData);
		
		// drop the entry if there are no prdeictions
		model.setColdStartStrategy("drop");
		
		//Dataset<Row> predictions = model.transform(holdoutData);
		//predictions.show();
		
		List<Row> recommendedList = model.recommendForAllUsers(5).takeAsList(5);
		
		for(Row r : recommendedList) {
			System.out.println("User "+r.getAs(0)+" can be recommended course "+r.getAs(1));
			System.out.println("This user has already watched");
			inputData.filter("userId = "+r.getAs(0)).show();
		}
		
		//inputData.show();
				
		session.close();

	}

}
