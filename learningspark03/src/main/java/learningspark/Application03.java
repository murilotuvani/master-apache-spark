package learningspark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Application03 {

	public static void main(String args[]) {
		System.out.println("Application03");
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
	}

}
