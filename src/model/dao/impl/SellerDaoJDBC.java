package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import db.DB;
import db.DbException;
import model.dao.SellerDao;
import model.entities.Department;
import model.entities.Seller;

public class SellerDaoJDBC implements SellerDao {
	
	private Connection conn;
	
	public SellerDaoJDBC(Connection conn) {
		this.conn=conn;
	}

	@Override
	public void insert(Seller obj) {
		PreparedStatement statement=null;
		try {
			statement=conn.prepareStatement(
			"insert into seller "
			+ "(Name,Email,BirthDate, BaseSalary, DepartmentId) "
			+"values (?,?,?,?,?) ",
			Statement.RETURN_GENERATED_KEYS);
			
			statement.setString(1,obj.getName());
			statement.setString(2,obj.getEmail());
			statement.setDate(3, new java.sql.Date(obj.getBirthDate().getTime()));
			statement.setDouble(4, obj.getBaseSalaryDouble());
			statement.setInt(5, obj.getDepartment().getId());
			
			int rowsAffected =statement.executeUpdate();
			
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
	public void update(Seller obj) {
		PreparedStatement statement=null;
		try {
			statement=conn.prepareStatement(
			"Update seller "
			+ " set Name= ?,Email=?,BirthDate=?, BaseSalary=?, DepartmentId= ? "
			+ "where Id= ?"
			);
			statement.setString(1,obj.getName());
			statement.setString(2,obj.getEmail());
			statement.setDate(3, new java.sql.Date(obj.getBirthDate().getTime()));
			statement.setDouble(4, obj.getBaseSalaryDouble());
			statement.setInt(5, obj.getDepartment().getId());
			statement.setInt(6, obj.getId());
			
			statement.executeUpdate();
			
			
			
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		}finally {
			DB.closeStatement(statement);
		}
		}

	@Override
	public void deleteById(Integer id) {
		
	}

	@Override
	public Seller findById(Integer id) {
		PreparedStatement st= null;
		ResultSet rs=null;
		try {
			st=conn.prepareStatement(
					"SELECT seller.*, department.Name as DepName "
					 +"FROM seller INNER JOIN department "
					 +"ON seller.DepartmentId= department.Id "
					 +"WHERE seller.Id= ?");
			
			st.setInt(1, id);
			rs=st.executeQuery();
			if(rs.next()) {
				Department department=instantiateDepartment(rs);
				Seller objSeller=instantiateSeller(rs,department);
				return objSeller;
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

	private Department instantiateDepartment(ResultSet rs) throws SQLException {
		Department department=  new Department();
		department.setId(rs.getInt("DepartmentId")); 
		department.setName(rs.getString("DepName"));
		return department;
	}

	private Seller instantiateSeller(ResultSet rs, Department department) throws SQLException {
		Seller objSeller= new Seller();
		objSeller.setId(rs.getInt("Id"));
		objSeller.setName(rs.getString("Name"));
		objSeller.setEmail(rs.getString("Email"));
		objSeller.setBaseSalaryDouble(rs.getDouble("BaseSalary"));
		objSeller.setBirthDate(rs.getDate("BirthDate"));
		objSeller.setDepartment(department);
		return objSeller;
	}

	@Override
	public List<Seller> findAll() {
		PreparedStatement st= null;
		ResultSet rs=null;
		try {
			st=conn.prepareStatement(
					"select seller.*,department.Name as DepName "
				+	"from seller inner join department "
				+	"on seller.DepartmentId= department.Id "
				+	"order by Name " ); 
			
			rs=st.executeQuery();
			
			List<Seller> list=new ArrayList<>();
			Map<Integer, Department> map=new HashMap<>();
			
		while(rs.next()) {
				Department dep=map.get(rs.getInt("DepartmentId"));
				
				if(dep==null) {
					dep=instantiateDepartment(rs);
					map.put(rs.getInt("DepartmentId"), dep);
				}
				
				Seller obj=instantiateSeller(rs,dep);
				list.add(obj);
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
	
	@Override
	public List<Seller> findByDepartment(Department department) {
		PreparedStatement st= null;
		ResultSet rs=null;
		try {
			st=conn.prepareStatement(
					"select seller.*,department.Name as DepName "
				+	"from seller inner join department "
				+	"on seller.DepartmentId= department.Id "
				+	"where DepartmentId= ? "
				+	"order by Name " ); 
			
			st.setInt(1, department.getId());
			rs=st.executeQuery();
			
			List<Seller> list=new ArrayList<>();
			Map<Integer, Department> map=new HashMap<>();
			
		while(rs.next()) {
				Department dep=map.get(rs.getInt("DepartmentId"));
				
				if(dep==null) {
					dep=instantiateDepartment(rs);
					map.put(rs.getInt("DepartmentId"), dep);
				}
				
				Seller obj=instantiateSeller(rs,department);
				list.add(obj);
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
