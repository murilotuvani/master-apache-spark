package learningspark08;

import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class RegressaoLinear {

	public static void main(String args[]) {
		String file = "src/main/resources/marketing_vs_sales.csv";
		
		SparkSession spark = new SparkSession.Builder()
				.appName("LinarRegressionExample")
				.master("local")
				.getOrCreate();
		
		Dataset<Row> markVsSalesDf = spark.read()
				.option("header", "true")
				.option("inferSchema", "true")
				.format("csv")
				.load(file);
		
		// |marketing_spend|   sales|
		// marketing_spend eh uma feature
		// sales eh chamado label pois eh a saida do resultado
		// se fosse mais de uma variavel de entrada, entao deveria ter um array de features
		markVsSalesDf.show();
		
		Dataset<Row> mldf = markVsSalesDf.withColumnRenamed("sales", "label")
					.select("label", "marketing_spend");
		
		mldf.show();
		
		String[] features = new String[] {"marketing_spend"};
		VectorAssembler assembler = new VectorAssembler()
				.setInputCols(features)
				.setOutputCol("features");
		
		Dataset<Row> lblFeatures = assembler.transform(mldf).select("label", "features");
		
		// Remove as linhas com valores nulos
		lblFeatures = lblFeatures.na().drop();
		
		lblFeatures.show();
		
		LinearRegression lr = new LinearRegression();
		LinearRegressionModel learningModel = lr.fit(lblFeatures);
		
		
		//
		learningModel.summary().meanAbsoluteError();
		
		
		/**
		 * No caso eh possivel fazer analogias.
		 * Por exemplo, ao sabermos a quantidade de carros que uma cidade tem que usa tal disco de freio
		 * podemos simular quantos discos seriam vendidos em outra cidade.
		 */
		learningModel.summary().predictions().show();
		
		// Indice estatistico que exibe quao bem ficou
		// Quanto mais proximo de 1 mais forte a correlacao entre as variaveis
		System.out.println("R Squared : " + learningModel.summary().r2());
		
		
		/**
		 * Refazendo com mais uma variavel (feature
		 */
		String fileBadDay = "src/main/resources/marketing_vs_sales_bad_day.csv";
		Dataset<Row> markVsSalesDfBadDay = spark.read()
				.option("header", "true")
				.option("inferSchema", "true")
				.format("csv")
				.load(fileBadDay);
		
		markVsSalesDfBadDay.show();
		
		Dataset<Row> mldfBadDay = markVsSalesDfBadDay.withColumnRenamed("sales", "label")
					.select("label", "marketing_spend", "bad_day");
		
		mldfBadDay.show();
		
		// Especificando quais as colunas que sao variaveis f(marketing_spend, bad_day)=sales
		String[] featuresBadDay = new String[] {"marketing_spend", "bad_day"};
		VectorAssembler assemblerBadDay = new VectorAssembler()
				.setInputCols(featuresBadDay)
				.setOutputCol("features");
		
		Dataset<Row> lblFeaturesBadDay = assemblerBadDay.transform(mldfBadDay).select("label", "features");
		
		// Remove as linhas com valores nulos
		lblFeaturesBadDay = lblFeaturesBadDay.na().drop();
		
		lblFeaturesBadDay.show();
		
		LinearRegression lrBadDay = new LinearRegression();
		LinearRegressionModel learningModelBadDay = lrBadDay.fit(lblFeaturesBadDay);
		
		
		//
		learningModelBadDay.summary().meanAbsoluteError();
		
		
		/**
		 * No caso eh possivel fazer analogias.
		 * Por exemplo, ao sabermos a quantidade de carros que uma cidade tem que usa tal disco de freio
		 * podemos simular quantos discos seriam vendidos em outra cidade.
		 */
		System.out.println("Predição (Bad Day) : ");
		learningModelBadDay.summary().predictions().show();
		
		// Indice estatistico que exibe quao bem ficou
		// Quanto mais proximo de 1 mais forte a correlacao entre as variaveis
		System.out.println("R Squared (Bad Day) : " + learningModelBadDay.summary().r2());
		
				
		
		
				
	}

}
