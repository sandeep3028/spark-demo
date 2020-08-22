package org.spark.udemy.tutorial.ml;

import static org.apache.spark.sql.functions.col;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HousePricePredictorPipeLine {
	
	public static void main(String[] args) {
		
	    Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession session = SparkSession
									.builder()
									.appName("House Price Predictor")
									.master("local[*]")
									.getOrCreate();
		
		Dataset<Row> inputData = session
				.read()
				.option("inferSchema", true)
				.option("header", true)
				.csv("src/main/resources/kc_house_data.csv");
		
		//to know the percentage of above living.
		inputData = inputData.withColumn("sqft_above_percentage", col("sqft_above").divide(col("sqft_living")))
				.withColumnRenamed("price", "label");
		
		Dataset<Row>[] splitData = inputData.randomSplit(new double[] {0.8, 0.2});
		
		Dataset<Row> trainingAndTestData = splitData[0];
		Dataset<Row> holdoutData = splitData[1];
			
		/*
		 * Converting classification fields into one hot encode vector
		 */
		StringIndexer conditionIndexer = new StringIndexer()
				.setInputCol("condition")
				.setOutputCol("conditionIndex");
		
		StringIndexer gradeIndexer = new StringIndexer()
				.setInputCol("grade")
				.setOutputCol("gradeIndex");
		
		StringIndexer zipcodeIndexer = new StringIndexer()
				.setInputCol("zipcode")
				.setOutputCol("zipcodeIndex");
		
		OneHotEncoderEstimator estimator = new OneHotEncoderEstimator()
				.setInputCols(new String[] {"conditionIndex","gradeIndex","zipcodeIndex"})
				.setOutputCols(new String[] {"conditionVector","gradeVector","zipcodeVector"});

		VectorAssembler vectorAssembler = new VectorAssembler()
				.setInputCols(new String[] {"bedrooms","bathrooms","sqft_living","sqft_above_percentage","floors","waterfront","conditionVector","gradeVector","zipcodeVector"})
				.setOutputCol("features");
		
		LinearRegression linearRegression = new LinearRegression();		
		
		ParamGridBuilder builder = new ParamGridBuilder();		
		ParamMap[] paramMap = builder.addGrid(linearRegression.regParam(), new double[] {0.01, 0.1, 0.5})
			.addGrid(linearRegression.elasticNetParam(), new double[] {0, 0.5, 1})
			.build();
		
		TrainValidationSplit trainValidationSplit = new TrainValidationSplit()
				.setEstimator(linearRegression)
				.setEvaluator(new RegressionEvaluator().setMetricName("r2"))
				.setEstimatorParamMaps(paramMap)
				.setTrainRatio(0.8);
		
		Pipeline pipeline = new Pipeline();
		pipeline.setStages(new PipelineStage[] {conditionIndexer, gradeIndexer, zipcodeIndexer, estimator, vectorAssembler, trainValidationSplit});
		PipelineModel pipeLineModel = pipeline.fit(trainingAndTestData);
		TrainValidationSplitModel trainValidationSplitModel = (TrainValidationSplitModel)pipeLineModel.stages()[5];
		LinearRegressionModel model = (LinearRegressionModel)trainValidationSplitModel.bestModel();
		
		holdoutData = pipeLineModel.transform(holdoutData);
		holdoutData = holdoutData.drop("prediction");
		
		System.out.println("Training Data R2 : "+model.summary().r2()+" RMSE : "+model.summary().rootMeanSquaredError());
		System.out.println("Test Data R2 : "+model.evaluate(holdoutData).r2()+" RMSE : "+model.evaluate(holdoutData).rootMeanSquaredError());
		System.out.println("Coefficients : "+model.coefficients()+" Intercept : "+model.intercept());
		System.out.println("Regaparam : "+model.getRegParam()+" elasticnetparam : "+model.getElasticNetParam());
		
		
		session.close();
					
		//Dataset<Row> predictionData = linearRegressionModel.transform(testData);		
		//predictionData.show();

		
	}
	
}
