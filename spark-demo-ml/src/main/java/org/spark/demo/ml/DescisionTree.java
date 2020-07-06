package org.spark.demo.ml;

import java.util.Arrays;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.*;

public class DescisionTree {
	
	public static void main(String[] args) {
		
	    Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession session = SparkSession
									.builder()
									.appName("Descision Tree")
									.master("local[*]")
									.getOrCreate();
		
		//UDF for grouping countries into one group as spark ML allows only 32 in category
		session.udf().register("countryGrouping", country -> {
			List<String> topCountries =  Arrays.asList(new String[] {"GB","US","IN","UNKNOWN"});
			List<String> europeanCountries =  Arrays.asList(new String[] {"BE","BG","CZ","DK","DE","EE","IE","EL","ES","FR","HR","IT","CY","LV","LT","LU","HU","MT","NL","AT","PL","PT","RO","SI","SK","FI","SE","CH","IS","NO","LI","EU"});
			
			if (topCountries.contains(country)) return country; 
			if (europeanCountries .contains(country)) return "EUROPE";
			else return "OTHER";
		}, DataTypes.StringType);
		
		Dataset<Row> inputData = session
				.read()
				.option("inferSchema", true)
				.option("header", true)
				.csv("src/main/resources/vppFreeTrials.csv");
		
		//calling above UDF and adding label 
		inputData = inputData.withColumn("country", callUDF("countryGrouping", col("country")))
				.withColumn("label", when(col("payments_made").$greater$eq(lit(1)), 1).otherwise(lit(0)));
		
		//Indexing country string
		StringIndexer countryIndexer = new StringIndexer();
		countryIndexer.setInputCol("country");
		countryIndexer.setOutputCol("countryIndex");
		inputData = countryIndexer.fit(inputData).transform(inputData);
		
		//Converting datafields to vector
		VectorAssembler assembler = new VectorAssembler();
		assembler.setInputCols(new String[] {"countryIndex", "rebill_period", "chapter_access_count", "seconds_watched"});
		assembler.setOutputCol("features");
		inputData = assembler.transform(inputData);
		
		inputData = inputData.select("label","features");
			
		//seperating train and holdout data
		Dataset<Row>[] trainAndHoldoutData = inputData.randomSplit(new double[] {0.8, 0.2});
		Dataset<Row> trainData = trainAndHoldoutData[0];
		Dataset<Row> holdOutData = trainAndHoldoutData[1];
		
		//selecting algorithm
		DecisionTreeClassifier classifier = new DecisionTreeClassifier();
		classifier.setMaxDepth(3);
		
		//generating model
		DecisionTreeClassificationModel model = classifier.fit(trainData);
		
		//using model on holdoutdata
		Dataset<Row> prediction = model.transform(holdOutData);
		
		//printing the descison in form of if else.
		System.out.println(model.toDebugString());
		
		//Getting accuracy
		MulticlassClassificationEvaluator eval = new MulticlassClassificationEvaluator();
		eval.setMetricName("accuracy");
		
		System.out.println("Accuracy : "+eval.evaluate(prediction));
		
		System.out.println("\n=================================Random Forest==========================================\n");
		
		RandomForestClassifier classifier2 = new RandomForestClassifier();
		classifier2.setMaxDepth(3);
		
		RandomForestClassificationModel model2 = classifier2.fit(trainData);
		Dataset<Row> prediction2  = model2.transform(holdOutData);
		
		System.out.println(model2.toDebugString());	
		System.out.println("RandomForest Accuracy : "+eval.evaluate(prediction2));
				
		//inputData.show();
		
		session.close();

	}

}
