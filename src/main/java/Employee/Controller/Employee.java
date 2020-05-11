package Employee.Controller;

public class Employee {

    private String empId;
    private String name;

    Employee(String empId, String name){
        this.empId = empId;
        this.setName(name);
    }
    public String getId() {
        return empId;
    }

    public void setId(String id) {
        this.empId = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
