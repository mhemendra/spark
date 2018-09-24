package spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class StockMaxClosingPerStock {

	public static void maxClosingPerYear() {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Max Closing Price Per Stock");
		JavaSparkContext context = new JavaSparkContext(conf);
		try {
			JavaPairRDD<String, String> javaFileRDD = context.wholeTextFiles("D:/BigData/Data/*");

			JavaPairRDD<String, String> javaPairRDD = javaFileRDD.flatMapToPair(line -> Arrays
					.asList(line._2.split("[\\r\\n]+")).stream()
					.map(word -> new Tuple2<String, String>(
							line._1.toString().substring(line._1.lastIndexOf("/") + 1, line._1.indexOf(".")), word))
					.iterator());
			JavaPairRDD<Tuple2<String, Integer>, Float> javaFinalRDD = javaPairRDD
					.filter(line -> !line.toString().contains("Date")).mapToPair(line -> {
						String[] lineArray = line._2.split(",");
						Tuple2<String, Integer> innerTuple = new Tuple2<String, Integer>(line._1,
								Integer.parseInt(lineArray[0].split("-")[0]));
						Tuple2<Tuple2<String, Integer>, Float> retTuple = new Tuple2<Tuple2<String, Integer>, Float>(
								innerTuple, Float.parseFloat(lineArray[5]));
						return retTuple;
					});
			System.out.println("Output::" + javaFinalRDD.reduceByKey((x, y) -> Math.max(x, y)).collect());
		} finally {
			context.close();
		}
	}

}
