package main;

import java.io.IOException;

import org.apache.spark.sql.AnalysisException;

import spark.TotalClosingPerYear;

public class SparkMain {

	public static void main(String[] args) throws AnalysisException, IOException {
		TotalClosingPerYear.totalClosingPerYear();;
	}

}
