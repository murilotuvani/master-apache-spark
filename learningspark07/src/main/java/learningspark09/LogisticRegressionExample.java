package learningspark09;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LogisticRegressionExample {

	public static void main(String args[]) {
		String file = "src/main/resources/cryotherapy.csv";
		
		SparkSession spark = new SparkSession.Builder()
				.appName("LogisticRegressionExample")
				.master("local")
				.getOrCreate();
		
		Dataset<Row> treatmentDf = spark.read()
				.option("header", "true")
				.option("inferSchema", "true")
				.format("csv")
				.load(file);
		
		treatmentDf.show();
		
		// A coluna resutlado do tratamento eh a saida
		
		Dataset<Row> lblFeatureDf = treatmentDf.withColumnRenamed("Result_of_Treatment",  "label")
			// Deixando as colunas na ordem desejada
		 	.select("label", "sex", "age", "time", "Number_of_Warts", "Type", "Area");
		
		// Removendo dados invalidos
		lblFeatureDf.na().drop();
		
		
		lblFeatureDf.show();
		
		// O algoritimo nao traxa Strings apenas numeros
		// entao precisamos deste indexador para
		// transformar o generno em um valor numerico
		StringIndexer genderIndexer = new StringIndexer()
				.setInputCol("sex")
				.setOutputCol("sexIndex");
		
		// Definindo as features (variaveis)
		VectorAssembler assembler = new VectorAssembler()
				.setInputCols(new String[] {"sexIndex", "age", "time", "Number_of_Warts", "Type", "Area"})
				.setOutputCol("features");
		
		// Definindo que 70% serao os dados de treinamento e 30% serão dados de validação
		Dataset<Row>[] splitData = lblFeatureDf.randomSplit(new double[] {0.7, 0.3});
		Dataset<Row> trainingDf = splitData[0];
		Dataset<Row> testingDf = splitData[1];
		
		LogisticRegression logReg = new LogisticRegression();
		
		Pipeline pl = new Pipeline();
		// O Pipeline sabe em que ordem tem qeu obter quais dipos de objetos.
		pl.setStages(new PipelineStage[] {genderIndexer, assembler, logReg});
		
		PipelineModel model = pl.fit(trainingDf);
		Dataset<Row> results = model.transform(testingDf);
		
		results.show();
		
		
		

	}

}
