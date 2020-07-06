package org.spark.demo.ml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;

import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GymNumOfRepsPredictor {
	
	public static void main(String[] args) {
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession session = SparkSession
									.builder()
									.appName("Gym NumOfReps Predictor")
									.master("local[*]")
									.getOrCreate();
		
		Dataset<Row> inputData = session
				.read()
				.option("inferSchema", true)
				.option("header", true)
				.csv("src/main/resources/GymCompetition.csv");
		
		//inputData.printSchema();
		
		StringIndexer genderIndexer = new StringIndexer();
		genderIndexer.setInputCol("Gender");
		genderIndexer.setOutputCol("GenderIndex");
		inputData = genderIndexer.fit(inputData).transform(inputData);
		
		OneHotEncoderEstimator genderVectorEstimator = new OneHotEncoderEstimator();
		genderVectorEstimator.setInputCols(new String[] {"GenderIndex"});
		genderVectorEstimator.setOutputCols(new String[] {"GenderVector"});
		inputData = genderVectorEstimator.fit(inputData).transform(inputData);
		
		VectorAssembler vectorAssembler = new VectorAssembler();
		vectorAssembler.setInputCols(new String[] {"Age","Height","Weight","GenderVector"});
		vectorAssembler.setOutputCol("features");
		
		Dataset<Row> csvDataWithFeatures = vectorAssembler.transform(inputData);
		
		Dataset<Row> inputModel = csvDataWithFeatures.select(col("NoOfReps"), col("features")).withColumnRenamed("NoOfReps","label");
		
		LinearRegression linearRegression = new LinearRegression();
		LinearRegressionModel linearRegressionModel = linearRegression.fit(inputModel);
		
		
		System.out.println("Coeff "+linearRegressionModel.coefficients()+" Intercept "+linearRegressionModel.intercept());
		
		Dataset<Row> predictedModel = linearRegressionModel.transform(inputModel);
		
		predictedModel.show();
		
		session.close();
		
	}

}
