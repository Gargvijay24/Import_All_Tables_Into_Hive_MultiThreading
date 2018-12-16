package SqoopMultiThreading;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class HiveConnectionFactory {
	
	public static Connection createConnectionHive() {

		//confProp = new ConfigProperties();
		String hiveDB = CommonProperties.getProperties().getProperty("hiveDBName").replace("\"", "");
		String hiveServerName = "put server name here";
		String hiveServerPort = "10000";
		String URL = "jdbc:hive2://" + hiveServerName + ":" + hiveServerPort + "/" + hiveDB;
		String USER = "";
		String PASSWORD = "";
		String DRIVER_CLASS = "org.apache.hive.jdbc.HiveDriver";
		
		System.out.println("!!!!!!!!!!.............." );
		
		try {
			Class.forName(DRIVER_CLASS);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		Connection connection = null;
		try {
			connection = DriverManager.getConnection(URL, USER, PASSWORD);
			System.out.println("Printing connection string:::::::::::::::::  " + connection.toString());
		} catch (SQLException e) {
			System.out.println("ERROR: Unable to Connect to Database.");
			System.out.println("Print stack trace   " +e.getStackTrace().toString());
		}
		return connection;

	}
}
