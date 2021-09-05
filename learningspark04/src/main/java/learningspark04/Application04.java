package learningspark04;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Application04 {
	
	public static void main(String args[]) {
		SparkSession spark = new SparkSession.Builder()
				.appName("Array to Dataset<String>")
				.master("local")
				.getOrCreate();
		
		String []  stringList = new String [] {"Pilsen", "Lagger", "Pilsen", "APA", "IPA", "Bock" , "IPA", "Waiss", "Pilsen" };
		
		List<String> data = Arrays.asList(stringList);
		
		// Aqui eh dataset e nao um dataframe
		// Dataframe é Dataset<Row> quando for Dataset<T> eh dataset
		Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
		
		ds.printSchema();
		ds.show();
		
		// Devolve outro tipo
		Dataset<Row> df = ds.groupBy("value").count();
		
		df.printSchema();
//		ds.groupBy("");
		df.show();
		
		// Outro jeito de converter Dataset para Dataframe
		df = ds.toDF();
		df.printSchema();
		df.show();
		
		
		// Cria um Dataset a partir do Dataframe
		ds = df.as(Encoders.STRING());
		
		
	}

}
