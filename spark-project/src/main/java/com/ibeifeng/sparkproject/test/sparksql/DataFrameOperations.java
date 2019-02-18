package com.ibeifeng.sparkproject.test.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class DataFrameOperations {
	public static void main(String[] args) {
		//最新的版本都2.4了，这个还是1.5。1的版本，我的天
        SparkConf sparkConf = new SparkConf().setAppName("PeopleInfoCalculator").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        
        SQLContext sqlContext = new SQLContext(sc);
        
		DataFrame df = sqlContext.read().json("src/main/resources/sparksql/people.json");

		df.cache();
		
		df.show();
		
		df.printSchema();
		
		df.select("name").show();
		
		df.select(df.col("age").plus(1),df.col("name")).show();
		
		// Select people older than 21
		df.filter(df.col("age").gt(21)).show();
		
		df.groupBy("age").count().show();
		
	}
}
