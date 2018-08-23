package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkMaxClosingPerYear {

	public void maxClosingPerYear() {
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
			System.out.println("Output::" + javaPairRDD.reduceByKey((x, y) -> Math.max(x, y)).collect());
		} finally {
			context.close();
		}

	}

}
