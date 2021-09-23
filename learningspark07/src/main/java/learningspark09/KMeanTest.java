package learningspark09;

import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class KMeanTest {

	public static void main(String args[]) {
		String file = "src/main/resources/Wholesale_customers_data.csv";

		SparkSession spark = new SparkSession.Builder().appName("KMeanTest").master("local").getOrCreate();

		Dataset<Row> wholeSaleDf = spark.read().option("header", "true").option("inferSchema", "true").format("csv")
				.load(file);

		wholeSaleDf.show();

		Dataset<Row> featuresDf = wholeSaleDf.select("channel", "Region", "Fresh", "Milk", "Grocery", "Frozen",
				"Detergents_Paper", "Delicassen");

		VectorAssembler assembler = new VectorAssembler().setInputCols(
				new String[] { "channel", "Fresh", "Milk", "Grocery", "Frozen", "Detergents_Paper", "Delicassen"})
				.setOutputCol("features");
		
		Dataset<Row> trainingData = assembler.transform(featuresDf).select("features");
		
		KMeans kmen = new KMeans().setK(3);
		KMeansModel model = kmen.fit(trainingData);
		
//		System.out.println(model.computeCost(trainingData));
		
		model.summary().predictions().show();
		
		

	}

}
