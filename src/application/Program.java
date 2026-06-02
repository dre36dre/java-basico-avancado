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
	
	System.out.println("\n==================Test 2: findAll =============");
	List<Department> list= departmentDao.findAll();
	for(Department d:list) {
		System.out.println(d);
	}
	
	System.out.println("\n=== TEST 3: insert =======");
	Department newDepartment=new Department(null,"Music");
	departmentDao.insert(newDepartment);
	System.out.println("Insert! New id: "+newDepartment.getId() );
	
	
	System.out.println("\n===============TEST 4: UPDATE==================");
	Department department2=departmentDao.findById(1);
	department2.setName("Food");
	departmentDao.update(department2);
	System.out.println("Update completed");
	
	
	System.out.println("\n===============TEST: DELETE=====================");
	System.out.println("Enter id for delete test: ");
	int id= scanner.nextInt();
	departmentDao.deleteById(id);
	System.out.println("Delete completed");
	scanner.close();
}
}
