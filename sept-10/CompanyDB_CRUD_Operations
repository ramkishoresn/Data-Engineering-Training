Use company_db;

db.employees.insertMany([
  { first_name: "Amit", last_name: "Verma", age: 28, department: "IT", salary: 50000 },
  { first_name: "Sneha", last_name: "Reddy", age: 21, department: "HR", salary: 35000 },
  { first_name: "Manoj", last_name: "Sharma", age: 27, department: null, salary: 40000 }, // no dept
  { first_name: "Priya", last_name: "Sethi", age: 26, department: "Marketing", salary: 32000 },
  { first_name: "Arjun", last_name: "Mehta", age: 25, department: "IT", salary: 74900 },
  { first_name: "Simran", last_name: "Kapoor", age: 44, department: "Operations", salary: 50000 },
  { first_name: "Neha", last_name: "Kukarni", age: 33, department: "HR", salary: 55000 },
  { first_name: "Vikram", last_name: "Menon", age: 41, department: "Sales", salary: 42000 }  // no dept
]);

clr

db.departments.insertMany([
  { dept_name: "IT", location: "Bangalore" },
  { dept_name: "HR", location: "Hyderabad" },
  { dept_name: "Finance", location: "Mumbai" },     // no employees
  { dept_name: "Marketing", location: "Delhi" },
  { dept_name: "Operations", location: "Chennai" },
  { dept_name: "R&D", location: "Pune" }            // no employees
]);

clr

db.employees.find();

db.employees.find({ salary: {$gt: 60000}});

db.employees.find({ department: null });

db.departments.find();

db.employees.updateOne({ first_name: "Ravi", last_name: "Sharma"},{$set: { department: "Finance"}});

db.employees.updateMany({department: "IT"},{$mul: {salary: 1.10}});

db.employees.deleteOne({first_name: "Priya", last_name: "Singh"});

db.employees.deleteMany({ department: null });

db.employees.aggregate([{ $group: { _id: "$department", count: { $sum: 1}}}]);

db.employees.aggregate([{ $group: { _id: "$department", avgSalary: { $avg: "$salary}}}]);

db.employees.aggregate([{ $group: { _id: "$department", maxSalary: { $max: "$salary}}}]);
