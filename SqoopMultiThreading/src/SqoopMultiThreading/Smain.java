package SqoopMultiThreading;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;



public class Smain {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
	
		BufferedReader counterReader = null;
		BufferedReader fileReader = null;
		int flag  = 0;
		flag = Integer.parseInt(CommonProperties.getProperties().getProperty("IncrementalLoad").replace("\"", ""));
		//define number of Thread		
		ExecutorService executor = Executors.newFixedThreadPool(Integer.parseInt(CommonProperties.getProperties().getProperty("numberOfThread")));
		
		Connection connect = HiveConnectionFactory.createConnectionHive();
		
		try {
	
			Path FileToRead=new Path(CommonProperties.getProperties().getProperty("hdfsFilePath").replace("\"", ""));
						 
	        FileSystem hdfs;
	        
	        FSDataInputStream fis = null;
	        FSDataInputStream fis1 = null;
	        
			try {
				hdfs = FileToRead.getFileSystem(new Configuration());
			          
		        fis = hdfs.open(FileToRead);
		        fis1 = hdfs.open(FileToRead);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}  
			
	        counterReader = new BufferedReader(new InputStreamReader(fis));
	        
	        int numberOfLine = 0;
	        String line = null;
	        String hql = null;
	        
	        if (flag == 0)
	        {
	        try {
	            while ((line = counterReader.readLine()) != null) {
	                numberOfLine++;
	            }
	           
	            fileReader =  new BufferedReader(new InputStreamReader(fis1));
	            
	            while ((line = fileReader.readLine()) != null) {

	            	//Assign work to threads
	                Runnable worker = new SqoopThreading(line);
	    			executor.execute(worker);
	
	            }
	            executor.shutdown(); 
	            // Wait until all threads are finish
	    		while (!executor.isTerminated()) 
	    		{
	    
	    		}
	    		System.out.println("Finished all threads");
	            
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	       }
	        else if (flag == 1)
	        {
	        try {
	            while ((line = counterReader.readLine()) != null) {
	                numberOfLine++;
	            }
	           
	            fileReader =  new BufferedReader(new InputStreamReader(fis1));
	            
	            while ((line = fileReader.readLine()) != null) {
	            	

	            	
	            	Statement state = connect.createStatement();
	            	System.out.println("!!!!!!!!!!Running 1st query!!!!!!!!!!");
	            	
	            	hql = "select max( " + line.split(",")[1].trim() + " ) from " + CommonProperties.getProperties().getProperty("hiveDBName").replace("\"", "") + "." + line.split(",")[0].trim();
	            	 
	            	System.out.println(hql);
	            	
	            	 ResultSet show_tables = state.executeQuery(hql);
	            	    while (show_tables.next()) {
	            	        System.out.println(show_tables.getString(1));
	            	    }
	            	
	            		
	            			
	            			
	            	String command = "sqoop import --connect " + CommonProperties.getProperties().getProperty("oracleConString").replace("\"", "") +
	            			" --username " + CommonProperties.getProperties().getProperty("userName").replace("\"", "") + " --password " +  
	            			CommonProperties.getProperties().getProperty("password").replace("\"", "") + " --table " + line.split(",")[0].trim() + 
	            			" --hive-database " + CommonProperties.getProperties().getProperty("hiveDBName").replace("\"", "") +
	            			" --hive-table " + line.split(",")[0] + " --target-dir " + CommonProperties.getProperties().getProperty("HiveWareHousePath").replace("\"", "") + 
	            			CommonProperties.getProperties().getProperty("hiveDBName").replace("\"", "") + ".db/" + line.split(",")[0].trim() +
	            			" --incremental append " + " --check-column " +  line.split(",")[1].trim() + " --last-value " + show_tables.getString(1) + " --fields-terminated-by ',' -m 1 --append ";


	            	//String command = "ps - ef";
	            	//Assign work to threads
	                Runnable worker = new SqoopThreading(command);
	    			executor.execute(worker);
	
	            }
	            executor.shutdown(); 
	            // Wait until all threads are finish
	    		while (!executor.isTerminated()) 
	    		{
	    
	    		}
	    		System.out.println("Finished all threads");
	            
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	       }
	        
	        else if (flag == 2)
	        {
	        try {
	            while ((line = counterReader.readLine()) != null) {
	                numberOfLine++;
	            }
	           
	            fileReader =  new BufferedReader(new InputStreamReader(fis1));
	            
	            while ((line = fileReader.readLine()) != null) {
	            	
	            	String command = "sqoop import --connect " + CommonProperties.getProperties().getProperty("oracleConString").replace("\"", "") + 
	            			" --username " + CommonProperties.getProperties().getProperty("userName").replace("\"", "") + " --password " +
	            			 CommonProperties.getProperties().getProperty("password").replace("\"", "") + " --table " + line.split(",")[0].trim() +
	            			" --check-column " + line.split(",")[2].trim() + " --incremental lastmodified --null-string ' ' --null-non-string ' ' --last-value " +
	            			" 02-APR-2012 --m 1 --target-dir " + "'" + CommonProperties.getProperties().getProperty("HiveWareHousePath").replace("\"", "") +
	            			line.split(",")[3].trim() + "/" + line.split(",")[0].trim() + "' --merge-key " + line.split(",")[1].trim();

	            	//Assign work to threads
	                Runnable worker = new SqoopThreading(command);
	    			executor.execute(worker);
	
	            }
	            executor.shutdown(); 
	            // Wait until all threads are finish
	    		while (!executor.isTerminated()) 
	    		{
	    
	    		}
	    		System.out.println("Finished all threads");
	            
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	       }

	    }

	    catch (Exception e) {
	        System.out.println("Unable to read file");
	    }
		
	}

}
