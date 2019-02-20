package com.ibeifeng.sparkproject.project;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.ibeifeng.sparkproject.util.IOUtils;

import scala.Tuple2;

public class KKAnalyzeBhCount {
public static void main(String[] args) {
		
		SparkConf sparkConf = new SparkConf().setAppName("JTKK").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> kkLineStr = sc.textFile("E:\\\\jtkk\\\\jtkk.txt_30.txt");
		//1,统计所有卡口的通过总量
		//TODO 中文字符编码是乱的
		JavaRDD<String> kkbhData = kkLineStr.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split("	")[2]);
            }
        });
		/*
		 * 
		 和上面的效果是一样的
		  JavaRDD<String> kkbhDataMap = kkLineStr.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                return s.split("	")[2];
            }
        });*/
		JavaPairRDD<Integer, Integer> kkbhKey = kkbhData.mapToPair(new PairFunction<String,Integer,Integer>() {
			@Override
			public Tuple2<Integer, Integer> call(String t) throws Exception {
				return new Tuple2<Integer, Integer>(Integer.parseInt(t), 1);
			}
		});
		JavaPairRDD<Integer, Integer> kkbhKeyCounts = kkbhKey.reduceByKey( new Function2<Integer, Integer, Integer>() {
		    @Override public Integer call(Integer i1, Integer i2) throws Exception {
		      return i1 + i2;
		    }
		  });
		JavaPairRDD<Integer, Integer> kkbhKeyCountSort = kkbhKeyCounts.sortByKey();
		showFormatJsonStr(kkbhKeyCountSort.collect());
	}
//打印数组
	private  static <T> void printArrayList(List<T> lists) {
		for(T list:lists) {
			System.out.println(list);
		}
	}
	private static void showFormatJsonStr(List<Tuple2<Integer, Integer>>  lists) {
		StringBuffer beginAndEnd = new StringBuffer("[");
		for(Tuple2<Integer, Integer> list:lists) {
			beginAndEnd.append("{" + 
					"\"name\": \""+list._1+"\",\r\n" + 
					"\"value\": \""+list._2+"\"\r\n" + 
					"},");
		}
		beginAndEnd.append("]");
		String jsonStr = beginAndEnd.toString().replace(",]", "]");
		IOUtils.outPutFile(jsonStr,"node");
	}
}
