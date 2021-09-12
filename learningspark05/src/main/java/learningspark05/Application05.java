package learningspark05;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Application05 {

	public static void main(String[] args) {
		SparkSession spark = new SparkSession.Builder()
				.appName("Learning Spark SQL Dataframe API")
				.master("local")
				.getOrCreate();
		String studentsFile = "src/main/resources/students.csv";
		Dataset<Row> studentDf = spark.read().format("csv")
			 .option("inferSchema", "true")
			 .option("header", true)
			 .load(studentsFile);
		studentDf.printSchema();
		studentDf.show();
		
		String gradeChatFile = "src/main/resources/grade_chart.csv";
		Dataset<Row> gradesDf = spark.read().format("csv")
				 .option("inferSchema", "true")
				 .option("header", true)
				 .load(gradeChatFile);
		
		gradesDf.printSchema();
		gradesDf.show();
		
		// Fazendo o JOIN dos dois Dataframes especificando as colunas GPA e gpa como juncao
		Dataset<Row> studentsGradesDf = studentDf.join(gradesDf, studentDf.col("GPA").equalTo(gradesDf.col("gpa")))
				.drop(gradesDf.col("gpa"));
		studentsGradesDf.show();
		
//		//student_id,student_name,State,GPA,favorite_book_title,working
//		//gpa,letter_grade
//		Dataset<Row> smallerDf = studentsGradesDf
//				               .filter(studentsGradesDf.col("gpa").between(2,  3.5))
//				               .select(studentsGradesDf.col("student_name")
//		                              ,studentsGradesDf.col("favorite_book_title")
//		                              ,studentsGradesDf.col("letter_grade"));

		//Vamos ficar com uma versao do codigo reduzida
		Dataset<Row> smallerDf = studentsGradesDf
	               .filter(studentsGradesDf.col("gpa").between(2,  3.5))
	               .select("student_name","favorite_book_title","letter_grade");
		smallerDf.show();
		

		//Um versao mais "SQL" utilizando o WHERE
		smallerDf = studentsGradesDf
	               //.where(studentsGradesDf.col("gpa").between(2,  3.5))
				   .where(studentsGradesDf.col("gpa").gt(3.0).and(studentsGradesDf.col("gpa").lt(4.5))
						   .or(studentsGradesDf.col("gpa").equalTo(1.0))						   )
	               .select("student_name","favorite_book_title","letter_grade");
		smallerDf.show();
		

		


	}

}
