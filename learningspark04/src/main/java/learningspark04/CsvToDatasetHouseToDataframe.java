package learningspark04;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import learningspark04.mapper.HouseMapper;
import learningspark04.pojo.House;

public class CsvToDatasetHouseToDataframe {

	public static void main(String args[]) {
		
		CsvToDatasetHouseToDataframe ctd = new CsvToDatasetHouseToDataframe();
		ctd.start();

	}

	private void start() {
		SparkSession spark = SparkSession.builder()
				.appName("CSV to dataframe to Dataset<House> and back")
				.master("local").getOrCreate();
		
		String filename = "src/main/resources/houses.csv";
		Dataset<Row> df = spark.read().format("csv")
				               .option("inferSchema", "true")
				               .option("header", true)
				               
				               .option("sep", ";") //
							   .option("dateFormat", "yyyy/MM/dd") //
								
				               .load(filename);
		
		df.printSchema();
		df.show();
		
		// df.map(Mapper, Encoder)
		Dataset<House> houseDs = df.map(new HouseMapper(), Encoders.bean(House.class));
		houseDs.printSchema();
		houseDs.show();
		
		Dataset<Row> dataframe = houseDs.toDF();
		dataframe.printSchema();
		dataframe.show();
		
		
		
	}
	
}
