package com.zendesk.maxwell.consumer;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MysqlWriter implements Runnable{
	private java.sql.Connection con = null;
	private PreparedStatement pst = null;
	private ResultSet rs = null;
	private String url = "jdbc:mysql://localhost:3306/employees";
	private String user = "root";
	private String password = "password";
	
	public void run(){
	try {
	     con = DriverManager.getConnection(url, user, password);
	     Statement st = (Statement) con.createStatement(); 
	     for(int i=501201;i<501301;i++)
	     st.executeUpdate("INSERT INTO employees VALUES ("+i+",'1986-02-12','Ash','Verma','M','2016-04-04')");
	     con.close();
	}catch (SQLException ex) {
		ex.printStackTrace();
	 } 
	}
	
	/*public static void main(String[] args){
		Thread t = new Thread(new MysqlWriter());
		t.start();
	}*/
}
