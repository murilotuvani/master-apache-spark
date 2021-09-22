package learningspark07;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class StreamingKafkaConsumer {

	public static void main(String[] args) throws StreamingQueryException {
		
		SparkSession spark = SparkSession.builder()
		        .appName("StreamingKafkaConsumer")
		        .master("local")
		        .getOrCreate();

		// Kafka Consumer
		Dataset<Row> messagesDf = spark.readStream()
		  .format("kafka")
		  .option("kafka.bootstrap.servers", "localhost:9092") // Especificando o endereco do servidor 
		  .option("subscribe", "test") // Topico onde serah feit a inscricao
		  .load()
		  // A linha abaixo so esta transformando em String o valor
		  .selectExpr("CAST(value AS STRING)"); // lines.selectExpr("CAST key AS STRING", "CAST value AS STRING") For key value
		
		// message.show() // <-- Can't do this when streaming!
		Dataset<String> words = messagesDf
				  .as(Encoders.STRING())
				  .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

		Dataset<Row> wordCounts = words.groupBy("value").count();
//
		
		
		try {
			StreamingQuery query = wordCounts.writeStream()
					.outputMode("append")
					.format("console")
					.start();
			query.awaitTermination();
		} catch (TimeoutException e) {
			e.printStackTrace();
		}
		

	}

}
