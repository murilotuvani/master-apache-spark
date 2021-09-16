package learningspark07;

import org.apache.spark.sql.SparkSession;

public class Application07 {

	public static void main(String[] args) {
		System.out.println("Hello!");
		
		SparkSession spark = new SparkSession.Builder().appName("From to DB").master("local").getOrCreate();
//		spark.read().jdbc(null, null, null, 0, 0, 0, null)

	}

}
