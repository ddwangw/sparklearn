package com.ibeifeng.sparkproject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.codehaus.janino.Java;

import scala.Tuple2;

public class RDDLearn {
	public static void main(String[] args0) {
		SparkConf conf = new SparkConf().setAppName("RDDLearn").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<String> list = new ArrayList();
		list.add("a");
		list.add("b");
		list.add("c");
		JavaRDD<String> temp = sc.parallelize(list);
		//上述方式等价于
		JavaRDD<String> temp2 = sc.parallelize(Arrays.asList("a","b","c"));
		
		List<String> listFilter=new ArrayList<String>();
		//建立列表，列表中包含以下自定义表项
		listFilter.add("error:a");
		listFilter.add("error:b");
		listFilter.add("error:c");
		listFilter.add("warning:d");
		listFilter.add("hadppy ending!");
		
		JavaRDD<String> lines = sc.parallelize(listFilter);
		//用java实现过滤器转化操作
		JavaRDD<String> errorLines = lines.filter(new Function<String,Boolean>(){
			@Override
			public Boolean call(String v) throws Exception {
				return v.contains("error");
			}
		});
		List<String> errorList = errorLines.collect();
		printArrayList(errorList);
		
		//合并操作：将两个RDD数据集合合并为一个RDD数据集
		JavaRDD<String> warningLines = lines.filter(new Function<String,Boolean>(){

			@Override
			public Boolean call(String v) throws Exception {
				return v.contains("warning");
			}
		});
		
		JavaRDD<String> unionLines = errorLines.union(warningLines);
		printArrayList(unionLines.collect());
		
		//获取前两个元素
		//warningLines 只有一个元素，超出了,即使只有一个也不会报错
		printArrayList(warningLines.take(2));
		
		printArrayList(unionLines.take(2));
		
		List<String> strLine=new ArrayList<String>();
		strLine.add("how are you");
		strLine.add("I am ok");
		strLine.add("do you love me");
		JavaRDD<String> input=sc.parallelize(strLine);
		
		//将文本的单词过滤出来
		JavaRDD<String> words = input.flatMap(new FlatMapFunction<String,String>(){
			@Override
			public Iterable<String> call(String v) throws Exception {
				return Arrays.asList(v.split(" "));
			}
		});
		printArrayList(words.collect());
		
		JavaPairRDD<String,Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String v) throws Exception {
				return new Tuple2(v,1);
			}
		});
		
		JavaPairRDD<String,Integer> results = counts.reduceByKey(new Function2<Integer,Integer,Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		printArrayList(results.collect());
	}
	//打印数组
	private  static <T> void printArrayList(List<T> lists) {
		for(T list:lists) {
			System.out.println(list);
		}
	}
}
