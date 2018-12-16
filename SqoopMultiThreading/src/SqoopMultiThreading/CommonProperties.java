package SqoopMultiThreading;

import java.io.IOException;
import java.util.Properties;

public class CommonProperties {
 
private static Properties prop = new Properties();
public static Properties getProperties() {
	
	try {
	    //load a properties file from class path, inside static method
	    prop.load(CommonProperties.class.getClassLoader().getResourceAsStream("config.properties"));

	    //get the property value and print it out
	    
	   
	} 
	catch (IOException ex) {
	    ex.printStackTrace();
	}
	return prop;
}
	
}
