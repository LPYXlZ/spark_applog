package com.lpy.spark_applog;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;


public class DBHelper {
	public static final String url ="jdbc:mysql://192.168.0.106:3306/spark?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
	public static final String name ="com.mysql.jdbc.Driver";
	public static final String user = "root";
	public static final String pasword = "root";
	
	public Connection connection = null;
	
	public DBHelper(){
		try {
			Class.forName(name);
			connection = DriverManager.getConnection(url,user,pasword);
			System.out.println(connection);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void insetData(String deviceId,String upTraffic,String downTraffic,String timeStamp){
		String sql = "insert into accesslog(deviceId,upTraffic,downTraffic,timeStamp) values (?,?,?,?)";
		try {
			PreparedStatement pt =connection.prepareStatement(sql);
			pt.setString(1, deviceId);
			pt.setString(2, upTraffic);
			pt.setString(3, downTraffic);
			pt.setString(4, timeStamp);
			pt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		
	}
	
	
	public void close(){
		try {
			this.connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	public static void main(String[] args) {
		 new DBHelper();
	}
}
