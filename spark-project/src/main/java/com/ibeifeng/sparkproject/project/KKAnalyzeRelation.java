package com.ibeifeng.sparkproject.project;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import com.ibeifeng.sparkproject.util.DateUtils;
import com.ibeifeng.sparkproject.util.IOUtils;

import scala.Tuple2;

public class KKAnalyzeRelation {
public static void main(String[] args) {
		
		SparkConf sparkConf = new SparkConf().setAppName("JTKK").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> kkLineStr = sc.textFile("E:\\\\jtkk\\\\jtkk.txt_30.txt");
		JavaPairRDD<String, String> kkCphmSet = kkLineStr.mapToPair(new PairFunction<String,String,String>(){
			@Override
			public Tuple2<String, String> call(String t) throws Exception {
				return new Tuple2<String,String>(t.split("	")[4],t);
			}
		});
		JavaPairRDD<String, Iterable<String>> kkCphmSetV = kkCphmSet.groupByKey();
		/*JavaPairRDD<String, Iterable<Tuple2<String, String>>> kkCphmSetv = kkCphmSet.groupBy(new Function<Tuple2<String,String>,String>(){
			@Override
			public String call(Tuple2<String, String> v1) throws Exception {
				// TODO Auto-generated method stub
				return v1._1;
			}
		});*/
		JavaPairRDD<String, Iterable<String>> top2score = kkCphmSetV.mapToPair(new PairFunction<Tuple2<String,Iterable<String>>, String, Iterable<String>>() {
			@Override
			public Tuple2<String, Iterable<String>> call(Tuple2<String, Iterable<String>> t) throws Exception {
				List<String[]> list = new ArrayList<String[]>();
				Iterator<String> it = t._2.iterator();
				while(it.hasNext()) {
					String score = it.next();
					String[] kkbhAndCrossTime = score.split("	");
					list.add(kkbhAndCrossTime);
				}
				Collections.sort(list, new Comparator<String[]>() {
						@Override
						public int compare(String[] arg0, String[] arg1) {
							Date one = DateUtils.parseTime(arg0[7]);
							Date two = DateUtils.parseTime(arg1[7]);
							return (int) (two.getTime()-one.getTime());
						}  
			        }); 
				Map<String,Integer> resultSet = new HashMap<String,Integer>();
				int tempNumber = 0;
				String tempTime = "";
				if(list.size()>1) {
					tempNumber = Integer.parseInt(list.get(0)[2]);
					tempTime = list.get(0)[7].split(" ")[0];
					for(int i = 1 ;i<list.size();i++) {
						int afterNumber = Integer.parseInt(list.get(i)[2]);
						String afterTime = list.get(i)[7].split(" ")[0];
						//同一天时间，且通过卡口编号不能相等
						if(afterNumber!=tempNumber&&tempTime.equals(afterTime)) {
							String keyStr = tempNumber+"-"+afterNumber;
							if(resultSet.containsKey(keyStr)) {
								resultSet.put(keyStr, resultSet.get(keyStr)+1);
							}else{
								resultSet.put(keyStr, 1);
							};
						}
						tempNumber = afterNumber;
						tempTime = afterTime;
					}
				}
				List<String> resultList = new ArrayList<String>();
				for (Map.Entry<String, Integer> entry : resultSet.entrySet()) {
					resultList.add(entry.getKey());
				 }
				return new Tuple2<String, Iterable<String>>(t._1,resultList);
			}
		});
			
		/*	List<Integer> list = new ArrayList<Integer>();
			Iterator<Integer> it = tuple._2.iterator();
			while(it.hasNext()) {
				Integer score = it.next();
				list.add(score);
			}
			
			Collections.sort(list, (v1, v2) -> -(v1.compareTo(v2)));
			
			return new Tuple2<String, Iterable<Integer>>(tuple._1,list);
		}*/
		showFormatJsonStr(top2score.collect());
	}
	//打印数组
	private  static <T> void printArrayList(List<T> lists) {
		for(T list:lists) {
			System.out.println(list);
		}
		System.out.println(lists.size());
	}
	//生成需要的json串
	private static void showFormatJsonStr(List<Tuple2<String, Iterable<String>>> lists) {
		Map<String,Integer> resultSet = new HashMap<String,Integer>();
		for(Tuple2<String, Iterable<String>> list:lists) {
			Iterator<String> it = list._2.iterator();
			while(it.hasNext()) {
				String score = it.next();
				String[] be = score.split("-");
				//判断起始卡口是否一致，不一致进行添加，一致就不做任何处理
				if(!be[0].equals(be[1])) {
					if(resultSet.containsKey(be[0]+"-"+be[1])||resultSet.containsKey(be[1]+"-"+be[0])) {
						continue;
					}else {
						resultSet.put(score, 1);
					}
				}
			}
		}
		StringBuffer beginAndEnd = new StringBuffer("[");
	//拿到不重复的数据进行拼接
		for (Map.Entry<String, Integer> entry : resultSet.entrySet()) {
			String[] be = entry.getKey().split("-");
			beginAndEnd.append("{" + 
					"\"source\": \""+be[0]+"\",\r\n" + 
					"\"target\": \""+be[1]+"\"\r\n" + 
					"},");
		 }
		beginAndEnd.append("]");
		String jsonStr = beginAndEnd.toString().replace(",]", "]");
		IOUtils.outPutFile(jsonStr,"link");
	}
}
