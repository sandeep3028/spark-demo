package org.spark.demo.ml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HousePriceFieldAnalysis {

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
			
			//Checking for range.
			//inputData.describe().show();
			
			//double corr = inputData.stat().corr("price", "sqft_living");
			//System.out.println(corr);
			
			//removing irrelevant column if there is no range.
			inputData = inputData.drop("id","date","waterfront","view","condition","grade","yr_renovated","zipcode","lat","long");
			
				for (String col : inputData.columns()) {
					//System.out.println("Correlation between price and "+col+" : "+inputData.stat().corr("price", col));
				}
				
			//removing the column that are having less correlation with price, mostly below 0.2		
			inputData = inputData.drop("sqft_lot","floors","yr_built","sqft_lot15");
			
			//checking correlation between features, one above 0.8 can be excluded carefully after onbervartion.
			for (String col1 : inputData.columns()) {
				for (String col2 : inputData.columns()) {
					System.out.println("Correlation between "+col1+" and "+col2+" : "+inputData.stat().corr(col1, col2));
				}
			}
		
		}

}
