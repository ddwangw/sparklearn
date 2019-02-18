package com.ibeifeng.sparkproject.test.sparksql;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.ibeifeng.sparkproject.entity.Person;

public class InteroperatingWithRDDs {
	public static void main(String[] args) {
		
		SparkConf sparkConf = new SparkConf().setAppName("PeopleInfoCalculator").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		SQLContext sqlContext = new SQLContext(sc);
		
		JavaRDD<String> peopleLineStr = sc.textFile("src/main/resources/sparksql/people.txt");
		
		JavaRDD<Person> peopleEntityRDD = peopleLineStr.map(new Function<String, Person>() {

			@Override
			public Person call(String peopleStr) throws Exception {
				String[] params = peopleStr.split(",");
				//构造Person参数
				Person person = new Person();
				person.setName(params[0]);
				person.setAge(Integer.parseInt(params[1].trim()));
				return person;
			}
		});
		
		DataFrame schemaPeople = sqlContext.createDataFrame(peopleEntityRDD, Person.class);
		
		schemaPeople.registerTempTable("people");
		
		DataFrame teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19");

		List<String> teenagerNames = teenagers.javaRDD().map(new Function<Row, String>() {
		  public String call(Row row) {
		    return "Name: " + row.getString(0);
		  }
		}).collect();
		
		for(String teenagerName : teenagerNames) {
			System.out.println(teenagerName);
		}
	}
}
