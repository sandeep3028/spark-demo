package org.spark.demo.ml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class KMeanClusterDemo {

	public static void main(String[] args) {
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession session = SparkSession
									.builder()
									.appName("KMeanClusterDemo")
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
		
		VectorAssembler assembler = new VectorAssembler();
		assembler.setInputCols(new String[] {"GenderVector","Age","Height","Weight","NoOfReps"});
		assembler.setOutputCol("features");
		inputData = assembler.transform(inputData).select("features");
		
		
		for(int clusterNum= 2; clusterNum<=8; clusterNum++) {
			
			System.out.println("\n=========== Cluster Number "+clusterNum+" =====================\n");
			
			KMeans kMeans = new KMeans();
			kMeans.setK(clusterNum);
			
			KMeansModel model = kMeans.fit(inputData);
			Dataset<Row> predictions = model.transform(inputData);
			
			System.out.println("=== Predictions ===");
			predictions.show();
			
			System.out.println("=== Cluster Center ===");
			Vector[] cluster = model.clusterCenters();
			for(Vector v : cluster) {
				System.out.println(v);
			}
			
			System.out.println("=== Cluster Groups ===");
			predictions.groupBy("prediction").count().show();
			
			System.out.println("SSE : "+model.computeCost(inputData));
			
			ClusteringEvaluator evaluator = new ClusteringEvaluator();
			System.out.println("Slihouette with squared euclidean distance is "+evaluator.evaluate(predictions));
			
			System.out.println("\n==============================================================\n");
		}

		
		//inputData.show();
		
		session.close();
		
	}

}
