package com.ibeifeng.sparkproject.test.streaming;
import java.util.Arrays;

import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;
public class StreamingListenPort {
	public static void main(String[] args) throws Exception {
		// Create a local StreamingContext with two working thread and batch interval of 1 second
		//这里setMaster参数必须为local[2]，应为这里要开启两个进程，一个发一个收，若用默认的local将接受不到数据。
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
		// Create a DStream that will connect to hostname:port, like localhost:9999
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
		// Split each line into words
		JavaDStream<String> words = lines.flatMap(
		  new FlatMapFunction<String, String>() {
		    @Override public Iterable<String> call(String x) {
		      return Arrays.asList(x.split(" "));
		    }
		  });
		
		// Count each word in each batch
		JavaPairDStream<String, Integer> pairs = words.mapToPair(
		  new PairFunction<String, String, Integer>() {
		    @Override public Tuple2<String, Integer> call(String s) throws Exception {
		      return new Tuple2<String, Integer>(s, 1);
		    }
		  });
		
		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
		  new Function2<Integer, Integer, Integer>() {
		    @Override public Integer call(Integer i1, Integer i2) throws Exception {
		      return i1 + i2;
		    }
		  });
	
		// Print the first ten elements of each RDD generated in this DStream to the console
		wordCounts.print();
		jssc.start();              // Start the computation
		jssc.awaitTermination();   // Wait for the computation to terminate
	}
}
