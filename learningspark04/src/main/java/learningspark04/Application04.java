package learningspark04;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Application04 {

	public static void main(String args[]) {
		SparkSession spark = new SparkSession.Builder().appName("Array to Dataset<String>").master("local")
				.getOrCreate();

		String[] stringList = new String[] { "Pilsen", "Lagger", "Pilsen", "APA", "IPA", "Bock", "IPA", "Waiss",
				"Pilsen" };

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

		// fazen com uma classe implementada
		Dataset<String> nds = ds.map(new StringMapper(), Encoders.STRING());
		nds.show();
		nds.printSchema();

		// Fazendo com Lambda
		Dataset<String> lds = ds.map((MapFunction<String, String>) row -> "Word : " + row, Encoders.STRING());
		lds.show();
		lds.printSchema();

		String rds = ds.reduce(new StringReducer());
		System.out.println("String reduzida : " + rds);
		// ds.reduce((ReduceFunction<String, String> row)

	}

	/**
	 * A funcao vai devolver para String passada a propria String em maiúscula com o
	 * menos (-) como separador
	 * 
	 * @author murilotuvani
	 *
	 */
	static class StringMapper implements MapFunction<String, String>, Serializable {

		private static final long serialVersionUID = 1672578416233555828L;

		@Override
		public String call(String value) throws Exception {
			return " - " + value.toUpperCase();
		}

	}

	static class StringReducer implements ReduceFunction<String>, Serializable {

		private static final long serialVersionUID = 324665457287356259L;

		@Override
		public String call(String v1, String v2) throws Exception {
			return v1 + ", " + v2;
		}

	}

}
