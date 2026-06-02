package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import db.DB;
import db.DbException;
import model.dao.DepartmentDao;
import model.entities.Department;

public class DepartmentDaoJDBC implements DepartmentDao {
	
	private Connection conn;
	
	public DepartmentDaoJDBC(Connection conn) {
		this.conn=conn;
	}

	@Override
	public void insert(Department obj) {
		PreparedStatement statement=null;
		try {
			statement=conn.prepareStatement(
			"insert into department "
			+ "(Name,DepartmentId) "
			+"values (?,?) ",
			Statement.RETURN_GENERATED_KEYS);
			
			statement.setString(1,obj.getName());
			statement.setInt(5, obj.getDepartment().getId());
			
			int rowsAffected =statement.executeUpdate();
			
			int rows=statement.executeUpdate();
			if(rows==0) {
				throw new SQLException("Nehuma linha foi alterada pela operação");
			}
			
			
			if(rowsAffected > 0) {
				ResultSet rs=statement.getGeneratedKeys();
				if(rs.next()) {
					int id=rs.getInt(1);
					obj.setId(id);
				}
				DB.closeResultSet(rs);
			}
			else {
				throw new DbException("Unexpected error! No rows affected!");
			}
			
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		}finally {
			DB.closeStatement(statement);
		}
	}

	@Override
	public void update(Department obj) {
		PreparedStatement statement=null;
		try {
			statement=conn.prepareStatement(
			"UPDATE department "
			+ " SET Name= ? "
			+ "WHERE Id= ?"
			);
			
			statement.setString(1,obj.getName());
			statement.setInt(2, obj.getId());
			
			statement.executeUpdate();
			
			
			
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		}finally {
			DB.closeStatement(statement);
		}
		
		}

	@Override
	public void deleteById(Integer id) {
		PreparedStatement statement= null;
		try {
			statement=conn.prepareStatement("DELETE FROM department WHERE Id=?");
			statement.setInt(1, id);
			statement.executeUpdate();
			
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(statement);
		}
	}

	@Override
	public Department findById(Integer id) {
		PreparedStatement st= null;
		ResultSet rs=null;
		try {
			st=conn.prepareStatement(
					"SELECT * FROM department WHERE Id= ?");
			
			st.setInt(1, id);
			rs=st.executeQuery();
			if(rs.next()) {
				Department department= new Department();
				department.setId(rs.getInt("Id"));
				department.setName(rs.getString("Name"));
				return department;
			}
			return null;
		} catch ( SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
		}
	}



	@Override
	public List<Department> findAll() {
		PreparedStatement st= null;
		ResultSet rs=null;
		try {
			st=conn.prepareStatement(
					"SELECT * FROM department ORDER By Name " ); 
			
			rs=st.executeQuery();
			
			List<Department> list=new ArrayList<>();
			
		while(rs.next()) {
				Department dep= new Department();
		
				dep.setId(rs.getInt("Id"));
				dep.setName(rs.getString("Name"));
				list.add(dep);

		}
			return list;
		} catch ( SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
		}
		
	}
	

}
