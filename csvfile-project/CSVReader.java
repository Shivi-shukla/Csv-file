package com.nt.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.DriverManager;

public class CSVReader extends CSVVar{
	
	CSVVar cv=new CSVVar();
	
	String CUSTOMER_INSERT_QUERY="INSERT INTO CSVREAD(STUDENT_NAME,COURSE,YEAR) VALUES(?,?,?)";

	String jdbcurl="jdbc:mysql://locahost:3306/employeesdb";
	String username="root";
	String password="root";
	
	//Here we create an object for csv file for reading purpose
	String file="D:\\CSVDoc\\comma-separted1.csv";
	
	int batchSize=20;
	
	//Connection con=null;
		
	try {
		//Established Connection
	   Connection con=DriverManager.getConnection(jdbcurl,username,password);
	   con.setAutoCommit(false);
	   
	   PreparedStatement ps=con.prepareStatement(CUSTOMER_INSERT_QUERY);
	   
	   //Read data from csv file 
	   BufferedReader br=new BufferedReader(new FileReader(file));
	   String lineText=null;
	   int count=0;
	   
	   br.readLine();
	   while((lineText=br.readLine())!=null) {
		String[] data=lineText.split(",");
		
		//Stored the given value
		ps.setString(1,cv.getStudent_name());
		ps.setString(2,cv.getCourse());
		ps.setInt(3,cv.getYear());
		ps.addBatch();
		
		if(count%batchSize==0) {
			ps.executeBatch();
		}
	   }
	   br.close();
	   ps.executeBatch();
	   con.commit();
	   con.close();
	   System.out.println("Data has been Inserted successfully");
	
	  }
       catch(Exception e) {
	   e.printStackTrace();
	}

}		
			