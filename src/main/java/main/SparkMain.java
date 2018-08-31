package main;
import org.apache.spark.sql.AnalysisException;

import spark.SparkCSV;

public class SparkMain {

	public static void main(String[] args) {
		try {
			SparkCSV.readCSV();
		} catch (AnalysisException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
