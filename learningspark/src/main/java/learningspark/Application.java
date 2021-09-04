package learningspark;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Application {

	public static void main(String args[]) throws InterruptedException {

		// Create a session
		SparkSession spark = new SparkSession.Builder().appName("CSV to DB").master("local").getOrCreate();

		// get data
		Dataset<Row> df = spark.read().format("csv").option("header", true)
				.load("src/main/resources/name_and_comments.txt");

		// Exibe o DataFrame com as tres primeiras linhas
		df.show(3);

		// Transformation
		// concat devolve uma coluna concatenando outras
		// cf.col() devolve o valor de outra coluna
		// lit() devolve um literal como uma coluna
		df = df.withColumn("full_name", concat(df.col("last_name"), lit(", "), df.col("first_name")));
		
		df.show();
		
		//criando um novo dataframe apenas para os registros
		//onde a coluna "comment" tenha números (expressao regular \d+)
		//este dataframe deve ser ordenado pela coluna 
		df = df.filter(df.col("comment").rlike("\\d+")).orderBy(df.col("last_name").asc());
		
		df.show(3);

		// Write to destination
//		if (false) {
			String dbConnectionUrl = "jdbc:mysql://localhost/spark_course_data"; // <<- You need to create this database
			Properties prop = new Properties();
			prop.setProperty("driver", "com.mysql.cj.jdbc.Driver");
			prop.setProperty("user", "root");
			prop.setProperty("password", "root"); // <- The password you used while installing Postgres

			df.write().mode(SaveMode.Overwrite).jdbc(dbConnectionUrl, "project1", prop);
//		} else {
//			String dbConnectionUrl = "jdbc:postgresql://localhost/spark_course_data"; // <<- You need to create this database
//			Properties prop = new Properties();
//			prop.setProperty("driver", "org.postgresql.Driver");
//			prop.setProperty("user", "postgres");
//			prop.setProperty("password", "postgres"); // <- The password you used while installing Postgres
//
//			df.write().mode(SaveMode.Overwrite).jdbc(dbConnectionUrl, "project1", prop);
//		}
		
	}
}
