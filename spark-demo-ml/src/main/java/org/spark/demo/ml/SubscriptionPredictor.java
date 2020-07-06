package org.spark.demo.ml;

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

import static org.apache.spark.sql.functions.*;

public class SubscriptionPredictor {

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
					.csv("src/main/resources/vppChapterViews");

			inputData = inputData
					.filter("is_cancelled = false")
					.withColumn("firstSub", when(col("firstSub").isNull(), 0).otherwise(col("firstSub")))
					.withColumn("all_time_views", when(col("all_time_views").isNull(), 0).otherwise(col("all_time_views")))
					.withColumn("last_month_views", when(col("last_month_views").isNull(), 0).otherwise(col("last_month_views")))
					.withColumn("next_month_views", when(col("next_month_views").isNull(), 0).otherwise(col("next_month_views")))
					.withColumnRenamed("next_month_views", "label");
			
			Dataset<Row>[] splitData = inputData.randomSplit(new double[] {0.9, 0.1});
			
			Dataset<Row> trainAndTestData = splitData[0];
			Dataset<Row> holdOutData = splitData[1];
			
			StringIndexer paymentMethodIndexer = new StringIndexer();
			paymentMethodIndexer.setInputCol("payment_method_type");
			paymentMethodIndexer.setOutputCol("payment_method_type_indexer");
			//inputData = paymentMethodIndexer.fit(inputData).transform(inputData);
			
			StringIndexer countryIndexer = new StringIndexer();
			countryIndexer.setInputCol("country");
			countryIndexer.setOutputCol("country_indexer");
			//inputData = countryIndexer.fit(inputData).transform(inputData);
			
			StringIndexer rebillPeriodIndexer = new StringIndexer();
			rebillPeriodIndexer.setInputCol("rebill_period_in_months");
			rebillPeriodIndexer.setOutputCol("rebill_period_in_months_indexer");
			//inputData = rebillPeriodIndexer.fit(inputData).transform(inputData);
			
			OneHotEncoderEstimator encoder = new OneHotEncoderEstimator();
			encoder.setInputCols(new String[] {"payment_method_type_indexer", "country_indexer", "rebill_period_in_months_indexer"});
			encoder.setOutputCols(new String[] {"payment_method_type_vector", "country_vector", "rebill_period_in_months_vector"});
			//inputData = encoder.fit(inputData).transform(inputData);
			
//			inputData = inputData.select(col("payment_method_type_vector"), col("country_vector"), col("rebill_period_in_months_vector"),col("firstSub"), col("age"), col("all_time_views"), col("last_month_views"), col("label"));
//			
//			for(String col : inputData.columns()) {
//				System.out.println("Corr between label and "+col+" : "+inputData.stat().corr("label", col));
//			}
			
			VectorAssembler assembler = new VectorAssembler();
			assembler.setInputCols(new String[] {"payment_method_type_vector", "country_vector", "rebill_period_in_months_vector","firstSub", "age", "all_time_views", "last_month_views"});
			assembler.setOutputCol("features");
			//inputData = assembler.transform(inputData);
			
			LinearRegression linearRegression = new LinearRegression();
			
			ParamMap[] paramMap = new ParamGridBuilder()
					.addGrid(linearRegression.regParam(), new double[] {0.01, 0.1, 0.5})
					.addGrid(linearRegression.elasticNetParam(), new double[] {0, 0.5, 1})
					.build();
			
			TrainValidationSplit trainValidationSplit = new TrainValidationSplit()
					.setEstimator(linearRegression)
					.setEvaluator(new RegressionEvaluator().setMetricName("r2"))
					.setEstimatorParamMaps(paramMap)
					.setTrainRatio(0.9);
			
			Pipeline pipeline = new Pipeline();
			pipeline.setStages(new PipelineStage[] {paymentMethodIndexer, countryIndexer, rebillPeriodIndexer, encoder, assembler, trainValidationSplit});
			PipelineModel pipeLineModel = pipeline.fit(trainAndTestData);
			TrainValidationSplitModel trainAndTestModel = (TrainValidationSplitModel)pipeLineModel.stages()[5];
			LinearRegressionModel model = (LinearRegressionModel)trainAndTestModel.bestModel();
			
			holdOutData = pipeLineModel.transform(holdOutData);
			holdOutData = holdOutData.drop("prediction");
			
			System.out.println("Training R2 : "+model.summary().r2()+" Training RMSE : "+model.summary().rootMeanSquaredError());
			System.out.println("Testing R2 : "+model.evaluate(holdOutData).r2()+" Testing RMSE : "+model.evaluate(holdOutData).rootMeanSquaredError());
			System.out.println("Coeff : "+model.coefficients()+" intercept : "+model.intercept());
			System.out.println("Regparam : "+model.getRegParam()+" Elasticnetparam : "+model.getElasticNetParam());

			//inputData.printSchema();
			//inputData.show();
			
			session.close();

	}

}
