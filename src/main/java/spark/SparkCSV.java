package spark;

import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkCSV {
	
	public static void readCSV() throws AnalysisException{
		SparkSession session = SparkSession.builder().master("local").appName("Spark SQL").getOrCreate();
		JavaRDD<String> stockRDD = session.sparkContext().textFile("D:/BigData/Data/NMDC.csv", 1).toJavaRDD();
		
		JavaRDD<Row> stockRow = stockRDD.filter(stock -> !stock.contains("Date")).map(row ->{
			String[] cols = row.split(",");
			return RowFactory.create(cols[0].trim(),cols[1].trim(),cols[2].trim(),cols[3].trim(),cols[4].trim(),cols[5].trim(),cols[6].trim(),cols[7].trim()					);
		});
		String[] schemaRow = stockRDD.first().split(",");
		List<StructField> structFieldList = new ArrayList<>();
		for(String fieldName : schemaRow){
			StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
			structFieldList.add(field);
		}
		StructType schema = DataTypes.createStructType(structFieldList);
		Dataset<Row> stockDf = session.createDataFrame(stockRow, schema);
		//stockDf.printSchema();
		//stockDf.write().mode("overwrite").format("csv").save("hdfs://localhost:50071/out/nmdc.csv");
		//stockDf.createGlobalTempView("nmdc");
		stockDf.select(col("Low").gt("100"),col("Date")).show();
		//JavaPairRDD<String,String> pairRDD = rowRDD.mapToPair(row -> new Tuple2<String,String>(row.get(0).toString(),row.get(1).toString()));
		//pairRDD.filter(line -> !line.contains("Date"));
		//Dataset<NSEStock> dataset = session.read().format("csv").option("header", false).load("D:/BigData/Data/NMDC.csv").as(Encoders.bean(NSEStock.class));
		//stockDf.foreach(stock -> System.out.println("Output:"+stock.getDate()));
	}
}
