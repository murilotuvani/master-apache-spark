package learningspark04;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import learningspark04.mapper.LineMapper;

public class Wordcount {

	public static void main(String args[]) {
		
		String boringWords = " ('a', 'an', 'and', 'are', 'as', 'at', 'be', 'but', 'by',\r\n" + 
		  		"'for', 'if', 'in', 'into', 'is', 'it',\r\n" + 
		  		"'no', 'not', 'of', 'on', 'or', 'such',\r\n" + 
		  		"'that', 'the', 'their', 'then', 'there', 'these',\r\n" + 
		  		"'they', 'this', 'to', 'was', 'will', 'with', 'he', 'she'," + 
		  		"'your', 'you', 'I', "
		  		+ " 'i','[',']', '[]', 'his', 'him', 'our', 'we') ";
		
		SparkSession spark = SparkSession.builder()
				.appName("Dados nao estruturados para flatmap")
				.master("local")
				.getOrCreate();
		
		String filename = "src/main/resources/shakespeare.txt";
		Dataset<Row> df = spark.read().format("text")
								
				               .load(filename);
		
		df.printSchema();
		df.show();
		
		Dataset<String> wordsDs = df.flatMap(new LineMapper(), Encoders.STRING());
		wordsDs.printSchema();
		wordsDs.show();
		
		Dataset<Row> ndf = wordsDs.toDF().groupBy("value").count();
		ndf = ndf.orderBy(ndf.col("count").desc());
		//Jogando para lower case para entao filtrar assim como se fosse SQL
		ndf.filter("lower(value) not in " + boringWords);
		ndf.printSchema();
		ndf.show();


	}
	
	

}
