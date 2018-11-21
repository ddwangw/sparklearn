package com.ibeifeng.sparkproject;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDLearn {
	public static void main(String[] args0) {
		SparkConf conf = new SparkConf().setAppName("RDDLearn").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<String> list = new ArrayList();
		list.add("a");
		list.add("b");
		list.add("c");
	}
}
