package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class TotalClosingPerYear {

	public static void totalClosingPerYear() {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Total Closing");
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
			Function<Float, Tuple2<Float, Integer>> initFn = x -> new Tuple2<Float, Integer>(x, 1);
			Function2<Tuple2<Float, Integer>, Float, Tuple2<Float, Integer>> mergeFn = (Tuple2<Float, Integer> x,
					Float y) -> {
				return new Tuple2<Float, Integer>(x._1 + y, x._2 + 1);
			};
			Function2<Tuple2<Float, Integer>, Tuple2<Float, Integer>, Tuple2<Float, Integer>> combineFn = (
					Tuple2<Float, Integer> x, Tuple2<Float, Integer> y) -> {
				return new Tuple2<Float, Integer>(x._1 + y._2, x._2 + y._2);
			};
			System.out.println("Output::" + javaPairRDD.combineByKey(initFn,mergeFn,combineFn).collect());
		} finally {
			context.close();
		}

	}

}
