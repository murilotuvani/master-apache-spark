package learningspark;

public class Application {

	public static void main(String args[]) {
		System.out.println("Olá");
		
		InferCSVSchema parser = new InferCSVSchema();
		parser.printSchema();
		
		DefineCSVSchema parser2 = new DefineCSVSchema();
		parser2.printDefinedSchema();
		
		JSONLinesParser parser3 = new JSONLinesParser();
		parser3.parseJsonLines();
	}

}
