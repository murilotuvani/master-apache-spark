package learningspark05;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class CustomersAndProducts {
	
	public static void main(String args[]) {
		SparkSession spark = new SparkSession.Builder()
				.appName("Learning Spark SQL Dataframe API")
				.master("local")
				.getOrCreate();
		String customersFile = "src/main/resources/customers.csv";
		Dataset<Row> customersDf = spark.read().format("csv")
			 .option("inferSchema", "true")
			 .option("header", true)
			 .load(customersFile);
		customersDf.printSchema();
		customersDf.show();
		
		
		String productsFile = "src/main/resources/products.csv";
		Dataset<Row> productsDf = spark.read().format("csv")
				 .option("inferSchema", "true")
				 .option("header", true)
				 .load(productsFile);
		
		productsDf.printSchema();
		productsDf.show();
		
		
		String purchasesFile = "src/main/resources/purchases.csv";
		Dataset<Row> purchasesDf = spark.read().format("csv")
				 .option("inferSchema", "true")
				 .option("header", true)
				 .load(purchasesFile);
		
		purchasesDf.printSchema();
		purchasesDf.show();
		
		Dataset<Row> joinedData = customersDf
				.join(purchasesDf,	customersDf.col("customer_id").equalTo(purchasesDf.col("customer_id")))
				.join(productsDf, purchasesDf.col("product_id").equalTo(productsDf.col("product_id")))
				.drop(purchasesDf.col("customer_id"))
				.drop(purchasesDf.col("product_id"))
				.drop("favorite_website", "product_id");
		joinedData.show();
		
//		joinedData.groupBy("first_name").count().show();
		
		// fazendo group by com duas colunas
		joinedData.groupBy("first_name", "product_name").agg(
				count("product_name").as("number_of_products"),
				max("product_price").as("most_exp_purchase"),
				sum("product_price").as("total_spent")
				).show();
		
		
		
	}

}
