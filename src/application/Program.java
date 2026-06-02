package application;

import java.util.List;
import java.util.Scanner;

import model.dao.DaoFactory;
import model.dao.DepartmentDao;
import model.entities.Department;

public class Program {
public static void main(String[] args) {
	Scanner scanner=new Scanner(System.in);
	
	DepartmentDao departmentDao=DaoFactory.createDepartmentDao();
	
	
	System.out.println("==================Test 1: findById ============");
	Department department=departmentDao.findById(4);
	System.out.println(department);
	
	System.out.println("==================Test 2: findAll =============");
	List<Department> list= departmentDao.findAll();
	for(Department d:list) {
		System.out.println(d);
	}
	
}
}
