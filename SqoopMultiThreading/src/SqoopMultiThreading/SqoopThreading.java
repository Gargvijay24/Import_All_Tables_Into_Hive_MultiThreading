package SqoopMultiThreading;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.tool.ImportTool;

public class SqoopThreading implements Runnable {
	   private Thread t;
	   private String threadName;
	   SqoopOptions options = new SqoopOptions();
	   String indexZeroTableName = null;
	   String indexOneSplitBy = null;
	   String indexTwoSize = null;
	   
	 
	public SqoopThreading(String sqoopQuery) {
		// TODO Auto-generated constructor stub
		threadName = sqoopQuery;
	    System.out.println("Creating " +  threadName );
	    
	}
	

	@Override
	public void run() {
	      
	      try {
	    	 
	    	int flag  = 0;
	  		flag = Integer.parseInt(CommonProperties.getProperties().getProperty("IncrementalLoad").replace("\"", ""));
	  		
	  		if (flag ==0)
	  		{
	  		
		    	  System.out.println("Running " +  threadName );
			      Thread.sleep(50); 
			      
			      String[] temp = threadName.split(",");
                  
                  indexZeroTableName = temp[0].trim();
                  indexOneSplitBy = temp[1].trim();
                  indexTwoSize = temp[2].trim();
		    	  		    	  
                  options.setConnectString(CommonProperties.getProperties().getProperty("oracleConString").replace("\"", ""));
                  options.setUsername(CommonProperties.getProperties().getProperty("userName").replace("\"", ""));
                  options.setPassword(CommonProperties.getProperties().getProperty("password").replace("\"", ""));
                             
		    	  options.setTableName(CommonProperties.getProperties().getProperty("Schema").replace("\"", "")+indexZeroTableName); 
			            
                  options.setNumMappers(Integer.parseInt(CommonProperties.getProperties().getProperty("numberOfMappers")));// Default value is 4         
                 
           
                  options.setHiveDatabaseName(CommonProperties.getProperties().getProperty("hiveDBName").replace("\"", ""));
                  
                  options.setHiveTableName(indexZeroTableName);
                  options.setOverwriteHiveTable(true);
                  options.setHiveImport(true);
                  options.setDirectMode(true);
                 
                  
			      int ret = new ImportTool().run(options);
			      
	  	}
	  		else if (flag ==1 || flag ==2)
	  		{
	  			System.out.println("Running " +  threadName );
			    Thread.sleep(50); 
			    
			    executeCommand(threadName);
			    //ProcessBuilder proc = new ProcessBuilder(threadName);
		        //proc.start();
	  		}
	  		
	  		
	      } catch (InterruptedException e) {
	         System.out.println("Thread " +  threadName + " interrupted.");
	      } catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	      
	     
	   }
	   
	   public void start () {
	     
	    	  System.out.println("Starting " +  threadName );
		      if (t == null) {
		         t = new Thread (this, threadName);
		         t.start ();
		         
		      
		      
    	  }
	   }
	   
	   static private String executeCommand(String command) {

			StringBuffer output = new StringBuffer();

			Process p;
			 String s = null;
			
			try {
				
				System.out.println("start.......!!!!!!!!!!");
				
				System.out.println(command);
				
				p = Runtime.getRuntime().exec(command);
				
				BufferedReader stdInput = new BufferedReader(new 
		                 InputStreamReader(p.getInputStream()));

		            BufferedReader stdError = new BufferedReader(new 
		                 InputStreamReader(p.getErrorStream()));

		            // read the output from the command
		            //System.out.println("Here is the standard output of the command:\n");
		            while ((s = stdInput.readLine()) != null) {
		                System.out.println(s);
		            }
		            
		            // read any errors from the attempted command
		            //System.out.println("Here is the standard error of the command (if any):\n");
		            while ((s = stdError.readLine()) != null) {
		                System.out.println(s);
		            }
		            
		            System.exit(0);
		            

			} catch (Exception e) {
				System.out.println("fat gaya!!!!!!!!!!!!!!!!!!!!!!!");
				e.printStackTrace();
			}
			System.out.println("finished!!!!!!!!!!");
			return output.toString();

		}


}



