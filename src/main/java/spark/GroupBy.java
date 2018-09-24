package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;
import scala.collection.mutable.HashSet;

public class GroupBy {
//https://dzone.com/articles/spark-pairrddfunctions-aggregatebykey;
	public static void maxClosingPerYear() {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Max Closing Price");
		JavaSparkContext context = new JavaSparkContext(conf);
		try {
			JavaRDD<String> javaFileRDD = context.textFile("D:/BigData/Data/*");
			JavaPairRDD<Integer, Float> javaPairRDD = javaFileRDD.filter(line -> !line.toString().contains("Date"))
					.mapToPair(line -> {
						String[] lineArray = line.split(",");
						Tuple2<Integer, Float> innerTuple = new Tuple2<Integer, Float>(
								Integer.parseInt(lineArray[0].split("-")[0]), Float.parseFloat(lineArray[5]));
						return innerTuple;
					});

			Function2<HashSet<Float>, Float, HashSet<Float>> mergeFn = (HashSet<Float> x,
					Float y) -> {return x+=y;};
	
					
					Function2<Tuple2<Float, Integer>, Tuple2<Float, Integer>, Tuple2<Float, Integer>> combineFn = (
					Tuple2<Float, Integer> x, Tuple2<Float, Integer> y) -> {
				return new Tuple2<Float, Integer>(x._1 + y._2, x._2 + y._2);
			};
			
			javaPairRDD.reduceByKey((x, y) -> {
				HashSet key = new HashSet();
			});
			System.out.println("Output::" + javaPairRDD.reduceByKey((x, y) -> Math.max(x, y)).collect());
		} finally {
			context.close();
		}

	}

}
