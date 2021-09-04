package learningspark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.lower;

public class Application03 {

	public static void main(String args[]) {
		System.out.println("Application03");
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		SparkSession spark = SparkSession.builder().appName("Combine 2 datasets").master("local").getOrCreate();

		Dataset<Row> durhamDf = buildDurhamParksDataFrame(spark);
		Dataset<Row> philDf = buildPhilParksDataFrame(spark);

		combineDataFrames(philDf, durhamDf);
	}

	/**
	 * Eh possivel combinar as colunas ordenadas mas neste caso vamos combinar pelo nome das colunas
	 * @param philDf
	 * @param durhamDf
	 */
	private static void combineDataFrames(Dataset<Row> philDf, Dataset<Row> durhamDf) {
		Dataset<Row> df = philDf.unionByName(durhamDf);
		df.show(500);
		df.printSchema();
		System.out.println("Nós temos "+df.count()+" registros");
	}

	private static Dataset<Row> buildPhilParksDataFrame(SparkSession spark) {
		// philadelphia_recreations.csv
		Dataset<Row> df = spark.read().format("csv").option("multiline", true).option("header", true)
				.load("src/main/resources/philadelphia_recreations.csv");

		df = df.filter(lower(df.col("USE_")).like("%park%"));
		//// a mesma coisa mas com SQL
		// df.filter("lower(USE_) like '%park%')

		df = df.withColumn("park_id", concat(lit("phil_"), df.col("OBJECTID")))
				// Mudando o nome de uma coluna para o novo Dataframe
				.withColumn("city", lit("Philadelphia")).withColumnRenamed("ADDRESS", "address")
				.withColumnRenamed("ASSET_NAME", "park_name").withColumn("has_playgroud", lit("unkown"))
				.withColumnRenamed("ZIPCODE", "zipcode").withColumnRenamed("ACREAGE", "land_in_acres")
				// Apenas preenchendo
				.withColumn("geoX", lit("unkown")).withColumn("geoY", lit("unkown"))
				// removendo os campos com os quais nao vamos trabalhar
				.drop("OBJECTID").drop("SITE_NAME").drop("CHILD_OF").drop("TYPE").drop("USE_").drop("DESCRIPTION")
				.drop("SQ_FEET").drop("ALLIAS").drop("CHRONOLOGY").drop("NOTES").drop("DATE_EDITED").drop("EDITED_BY")
				.drop("OCCUPANT").drop("TENANT").drop("LABEL");

		df.printSchema();
		df.show(300);

		return df;
	}

	private static Dataset<Row> buildDurhamParksDataFrame(SparkSession spark) {
		Dataset<Row> df = spark.read().format("json").option("multiline", true)
				.load("src/main/resources/durham-parks.json");
//		df.show(7, 90); // truncate after 90 chars
//		System.out.println("Dataframe's schema:");
//		df.printSchema();

		df = df.withColumn("park_id", concat(df.col("datasetid"), lit("_"), df.col("fields.objectid"), lit("_Durham")))
				.withColumn("park_name", df.col("fields.park_name")).withColumn("city", lit("Durham"))
				.withColumn("address", df.col("fields.address"))
				.withColumn("has_playgroud", df.col("fields.playground")).withColumn("zipcode", df.col("fields.zip"))
				.withColumn("land_in_acres", df.col("fields.acres"))
				// Pega so o primero X da lista
				.withColumn("geoX", df.col("geometry.coordinates").getItem(0))
				.withColumn("geoY", df.col("geometry.coordinates").getItem(1))
				// removendo os campos com os quais nao vamos trabalhar
				.drop("fields").drop("geometry").drop("record_timestamp").drop("recordid").drop("datasetid");
		;
		df.printSchema();
		df.show(10);
		return df;
	}

}
