package learningspark06;

import java.util.Arrays;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.desc;

public class Application06 {

	public static void main(String[] args) {
		SparkSession spark = new SparkSession.Builder().appName("Learning Spark SQL Dataframe API").master("local")
				.getOrCreate();
		// 16 segundos
//		String redditFile = "/Users/murilotuvani/spark/Reddit_2007-small.json";

//		// 45 segundos para executar
		String redditFile = "/Users/murilotuvani/spark/Reddit_2011-large.json";
		Dataset<Row> redditDf = spark.read().format("json").option("inferSchema", "true").option("header", true)
				.load(redditFile);

		redditDf.printSchema();
		redditDf.show();

		Dataset<Row> comentariosDf = redditDf.select("body");
		Dataset<String> palavrasDs = comentariosDf.flatMap((FlatMapFunction<Row, String>) r -> Arrays
				.asList(r.toString()
						.replace("\n", "")
						.replace("\r", "")
						.replace("]", " ")
						.replace("[", " ")
						.replace(".", " ")
						.replace("\s", " ")
						.trim().toLowerCase().split(" ")).iterator(),
				Encoders.STRING());
		Dataset<Row> palavrasDf = palavrasDs.toDF();
		Dataset<Row> palavrasAremover = spark.createDataset(Arrays.asList(WordUtils.stopWords), Encoders.STRING()).toDF();
		//Dataset<Row> palavrasContas = palavrasDf.except(palavrasAremover); // <-- remove as palavras duplicadas, entao nao da para contar as palavras com ele
		
		// entao a solucao eh fazer um LEFT JOIN removendo os casos crusados, isso eh feito com o tipo de join "leftanti"
		Dataset<Row> palavrasAContar = palavrasDf.join(palavrasAremover, palavrasDf.col("value").equalTo(palavrasAremover.col("value")), "leftanti");
		Dataset<Row> palavrasContadas = palavrasAContar.groupBy("value").count();
		palavrasContadas.orderBy(desc("count")).show();
		

	}

}
