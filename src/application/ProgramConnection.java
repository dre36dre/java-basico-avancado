package application;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import db.DB;
import db.DbException;

public class ProgramConnection {
public static void main(String[] args) {
	
	Connection conn=null;
	 Statement st=null;
	try {
		conn=DB.getConnection();
		
		conn.setAutoCommit(false);
		
		st=conn.createStatement();
		
		int rows1= st.executeUpdate("UPDATE seller SET BaseSalary= 5000 WHERE DepartmentId=1");
		
		//int x=1;
		//if(x<2) {
			//throw new SQLException("Fake error");
		//}
		int rows2= st.executeUpdate("UPDATE seller SET BaseSalary= 10000 WHERE DepartmentId=2");
		int rows3=st.executeUpdate("UPDATE seller SET BaseSalary=15000 WHERE DepartmentId=3");
		int rows4=st.executeUpdate("UPDATE seller SET BaseSalary=20000 WHERE DepartmentId=4");
		conn.commit();
				System.out.println("rows1 "+rows1);
				System.out.println("rows2 "+rows2);
				System.out.println("rows3 "+rows3);
				System.out.println("rows4 "+rows4);
	} catch (SQLException e) {
try {
	conn.rollback();
	throw new DbException("Transaction rolled  back! Caused by: "+e.getMessage());
} catch (SQLException e1) {
	throw new DbException("Error trying to rollback! Caused by: "+e.getMessage());
}
	}
	finally {
		DB.closeStatement(st);
		DB.closeConnection();
	}
	
}
}
