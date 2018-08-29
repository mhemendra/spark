package spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import pojo.NSEStock;

public class SparkCSV {
	
	public static void readCSV(){
		SparkSession session = SparkSession.builder().master("local").appName("Spark SQL").getOrCreate();
		Dataset<Row> dataset = session.read().format("csv").option("header", true).load("D:/BigData/Data/NMDC.csv");
		Encoder<NSEStock> stockEncoder = Encoders.bean(NSEStock.class); 
		Dataset<NSEStock> stockDS = dataset.as(stockEncoder);
		stockDS.filter(stock -> stock.getHighestPrice() == 105.95).show();
		stockDS.show();
	}
}
