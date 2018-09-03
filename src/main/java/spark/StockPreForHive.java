package spark;

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class StockPreForHive {

	public static void preProcess() {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Max Closing Price Per Stock");
		JavaSparkContext context = new JavaSparkContext(conf);
		try {
			JavaPairRDD<String, String> javaFileRDD = context.wholeTextFiles("D:/Downloads/*");
			JavaPairRDD<String, String> javaPairRDD = javaFileRDD.flatMapToPair(line -> Arrays
					.asList(line._2.split("[\\r\\n]+")).stream()
					.map(word -> new Tuple2<String, String>(
							line._1.toString().substring(line._1.lastIndexOf("/") + 1, line._1.indexOf(".")).split("-")[1], word))
					.iterator());
			javaPairRDD.filter(line->!line._2.contains("Date")).saveAsTextFile("D:/BigData/Data/");
		} finally {
			context.close();
		}
	}

}
