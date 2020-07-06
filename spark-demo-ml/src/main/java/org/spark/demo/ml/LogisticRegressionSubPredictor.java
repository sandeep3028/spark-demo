package org.spark.demo.ml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.LogisticRegressionSummary;
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

public class LogisticRegressionSubPredictor {

	public static void main(String[] args) {
		
		 Logger.getLogger("org.apache").setLevel(Level.WARN);
			
			SparkSession session = SparkSession
										.builder()
										.appName("House Price Predictor Logistic Regression")
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
					.withColumn("next_month_views", when(col("next_month_views").$greater(0), 0).otherwise(1))
					.withColumnRenamed("next_month_views", "label");
			
			StringIndexer paymentMethodIndexer = new StringIndexer();
			paymentMethodIndexer.setInputCol("payment_method_type");
			paymentMethodIndexer.setOutputCol("payment_method_type_indexer");
			inputData = paymentMethodIndexer.fit(inputData).transform(inputData);
			
			StringIndexer countryIndexer = new StringIndexer();
			countryIndexer.setInputCol("country");
			countryIndexer.setOutputCol("country_indexer");
			inputData = countryIndexer.fit(inputData).transform(inputData);
			
			StringIndexer rebillPeriodIndexer = new StringIndexer();
			rebillPeriodIndexer.setInputCol("rebill_period_in_months");
			rebillPeriodIndexer.setOutputCol("rebill_period_in_months_indexer");
			inputData = rebillPeriodIndexer.fit(inputData).transform(inputData);
			
			OneHotEncoderEstimator encoder = new OneHotEncoderEstimator();
			encoder.setInputCols(new String[] {"payment_method_type_indexer", "country_indexer", "rebill_period_in_months_indexer"});
			encoder.setOutputCols(new String[] {"payment_method_type_vector", "country_vector", "rebill_period_in_months_vector"});
			inputData = encoder.fit(inputData).transform(inputData);
			
			VectorAssembler assembler = new VectorAssembler();
			assembler.setInputCols(new String[] {"payment_method_type_vector", "country_vector", "rebill_period_in_months_vector","firstSub", "age", "all_time_views", "last_month_views"});
			assembler.setOutputCol("features");
			inputData = assembler.transform(inputData);
			
			inputData = inputData.select("label","features");
			
			Dataset<Row>[] splitData = inputData.randomSplit(new double[] {0.9, 0.1});
			
			Dataset<Row> trainAndTestData = splitData[0];
			Dataset<Row> holdOutData = splitData[1];
			
			LogisticRegression logisticRegression = new LogisticRegression();
			
			ParamMap[] paramMap = new ParamGridBuilder()
					.addGrid(logisticRegression.regParam(), new double[] {0.01, 0.1, 0.3, 0.5, 0.7, 1})
					.addGrid(logisticRegression.elasticNetParam(), new double[] {0, 0.5, 1})
					.build();
			
			TrainValidationSplit trainValidationSplit = new TrainValidationSplit()
					.setEstimator(logisticRegression)
					.setEvaluator(new RegressionEvaluator().setMetricName("r2"))
					.setEstimatorParamMaps(paramMap)
					.setTrainRatio(0.9);
			
			TrainValidationSplitModel trainAndTestModel = (TrainValidationSplitModel)trainValidationSplit.fit(trainAndTestData);
			LogisticRegressionModel model = (LogisticRegressionModel)trainAndTestModel.bestModel();			
			
			System.out.println("Training Accuracy : "+model.summary().accuracy());
			System.out.println("Coeff : "+model.coefficients()+" intercept : "+model.intercept());
			System.out.println("Regparam : "+model.getRegParam()+" Elasticnetparam : "+model.getElasticNetParam());			
			
			LogisticRegressionSummary summary = model.evaluate(holdOutData);
			double truePositive = summary.truePositiveRateByLabel()[1];
			double falsePositive = summary.falsePositiveRateByLabel()[0];
			
			System.out.println("In Holdoit data, Likelihood of positive being correct is "+ truePositive/(truePositive+falsePositive));
			System.out.println("Holdout Data accuracy : "+summary.accuracy());
			
			System.out.println("truePositive "+truePositive);
			System.out.println("falsePositive "+falsePositive);
			model.transform(holdOutData).groupBy("label","prediction").count().show();
						
			//inputData.printSchema();
			//inputData.show();
			
			session.close();

	}

}
