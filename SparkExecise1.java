package com.sparkTutorial.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.AbstractJavaRDDLike;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkExecise1 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf().setAppName("CarExample").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String>input = sc.textFile("/root/Documents/cars.csv");
		JavaRDD<String> carUSA = input.filter(line -> {
			String country = line.split(";")[8];
			return country.contains("US");
		});
		
		JavaRDD<String> OutContent = carUSA.map(line ->{
			String[] split = line.split(";");
			String carName = split[0];
			String MPG = split[1];
			return carName + "," + MPG;
		});	
	OutContent.saveAsTextFile("out/cars_output.text");
	}
}
