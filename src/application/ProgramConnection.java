package application;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import db.DB;
import db.DbIntegrityException;

public class ProgramConnection {
public static void main(String[] args) {
	
	Connection conn=null;
	PreparedStatement st=null;
	try {
		conn=DB.getConnection();
		
		st=conn.prepareStatement(
				"delete from seller "
				+"where "
				+ "id= ?");
		
		st.setInt(1,8 );
		
		int rowsAffected=st.executeUpdate();
		
		System.out.println("Done! Rows affected: " +rowsAffected);
				
	} catch (SQLException e) {
		throw new DbIntegrityException(e.getMessage());
	}
	finally {
		DB.closeStatement(st);
		DB.closeConnection();
	}
	
	
}
}
