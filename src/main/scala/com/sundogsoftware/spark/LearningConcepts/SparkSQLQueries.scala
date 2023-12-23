package com.sundogsoftware.spark.LearningConcepts
import com.sundogsoftware.spark.LearningConcepts.ParentSparkSQLQueriesDataFrame.createDataFrames_Worker_Bonus_Title
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{asc, col, concat, desc, length, lit, ltrim, regexp_replace, rtrim, substring, upper, count}

object SparkSQLQueries extends App {
  val spark = SparkSession.builder.master("local[*]").appName("SparkSQLQueries").getOrCreate
  import spark.implicits._

  spark.conf.set("spark.sql.codegen.wholeStage", false)

  val tableList = createDataFrames_Worker_Bonus_Title(spark)
    val worker = tableList(0)
    val bonus =  tableList(1)
    val title =   tableList(2)

  worker.createOrReplaceTempView("worker")
  bonus.createOrReplaceTempView("bonus")
  title.createOrReplaceTempView("title")

  //  -- Q-38. Write an SQL query to fetch workerId, full name, total income(salary + bonus), title of employees.
  var df = spark.sql("select workerId, concat(firstName,' ',lastName) as fullName, " +
    "sal + totalBonus as income, workerTitle from" +
    "(select first(w.workerId) as workerId, first(w.firstName) as firstName, " +
    "first(w.lastName) as lastName, first(w.department) as department, " +
    "first(b.bonusAmount) as bonusAmount, first(w.salary) as sal,  " +
    "first(t.workerTitle) as workerTitle, sum(bonusAmount)  as totalBonus from worker w " +
    "INNER JOIN bonus b on w.workerId = b.workerRefId " +
    "INNER JOIN title t on w.workerId = t.workerRefId " +
    "group by workerId) as sub")
  df.show
  df.explain()
  println(df.rdd.toDebugString)
//
//  -- Q -45.Write an SQL query to print the names of employees having the highest salary in each department
//  -- using dense_rank()
  var df1 = spark.sql("select * from(select firstName, lastName, department, salary, " +
  "dense_rank() over (partition by department order by salary DESC) as denseRank from worker) " +
  "as sub where denseRank = 1 order by salary DESC")
  df1.show
  df1.explain()
  println(df1.rdd.toDebugString)

//  -- Q-1. Write an SQL query to fetch “FIRST_NAME” from the Worker table using the alias name <WORKER_NAME>.
//    var q1 = spark.sql("select firstName as Worker_Name from worker w ")
      var q1 = worker.select("firstName").as("Worker_Name")
  q1.show()
  q1.explain()
  println(q1.rdd.toDebugString)

  // Using SQL Query
 //    -- Q-2. Write an SQL query to fetch “FIRST_NAME” from the Worker table in upper case.
 //  var q2 = spark.sql("select UPPER(firstName)as First_Name from worker")
  //Using SQL Functions
  var q2 = worker.select(upper(col("firstName")))
  q2.show()
  q2.explain()
  println(q2.rdd.toDebugString)

//    -- Q-3. Write an SQL query to fetch unique values of DEPARTMENT from the Worker table.
  //Using SQL Query
    var q3 = spark.sql("select Distinct(department) from worker")
  //Using SQL Functions
  var q3  = worker.select("department").distinct()
     q3.show()

//    -- Q-4. Write an SQL query to print the first three characters of  FIRST_NAME from the Worker table.
//    select workerId, firstName, lastName, salary, joiningDate, department, substring(firstName,1,3) as subFirstName  from worker
      var q4 = spark.sql("select workerId, firstName, lastName, salary, joiningDate, department, substring(firstName,1,3) as subFirstName  from worker")
  // Using SQL Functions
  worker.select(col("workerId"), col("firstName"), col("lastName"),
    col("salary"), col("joiningDate"), col("department"),
    substring(col("firstName"), 1, 3).as("subFirstName"))

  //  -- Q-6. Write an SQL query to print the FIRST_NAME from the Worker table after removing white spaces from the right side.
  spark.sql("select workerId, firstName, lastName, salary, joiningDate, department, " +
    "substring(firstName,1,3) as subFirstName  from worker")
  worker.select(rtrim(col(("firstName"))))
//    -- Q-7. Write an SQL query to print the DEPARTMENT from the Worker table after removing white spaces from the left side.
    spark.sql("select LTRIM(department) from worker")
  worker.select(ltrim(col(("firstName"))))

//    -- Q-8. Write an SQL query that fetches the unique values of DEPARTMENT from the Worker table and prints its length.
    spark.sql("select distinctValues, length(distinctValues) as len from " +
      "(select DISTINCT(department) as distinctValues from worker) as subQuery")
  var subQuery = worker.select(col("department").as("distinctValues")).distinct()
  var q8 = subQuery.select(col("distinctValues"), length(col("distinctValues").as("len")))
  q8.show

//    -- Q-9.  print the FIRST_NAME from the Worker table after replacing ‘a’ with ‘A’.
  spark.sql("select replace(firstName, 'a', upper('a')) from worker")

//    -- Q-10. print the FIRST_NAME and LAST_NAME from the Worker table into a single column COMPLETE_NAME. A space char should separate them.
  spark.sql("select concat(firstName, ' ', lastName) as CompleteName from Worker")
  var q10 = worker.select(concat(col("firstName"), lit(' '), col("lastName")).as("CompleteName"))
  q10.show

//  -- Q -11.Write an SQL query to print all Worker details from the Worker table order by FIRST_NAME Ascending
  spark.sql("select * from worker order by firstName")
  val q11 = worker.select(col("*")).orderBy("firstName")
  q11.show

//  -- Q -12.Write an SQL query to print all Worker details from the Worker table order by FIRST_NAME Ascending and DEPARTMENT Descending.
  spark.sql("select * from worker order by firstNamE asc,department desc")
  val q12 = worker.select(col("*")).orderBy(asc("firstName"), desc("department"))
  q12.show

//  -- Q -13.Write an SQL query to print details for Workers with the first names “Vipul ”and “Satish” from the Worker table
  spark.sql("select * from worker where firstName in('Vipul', 'Satish')")
  val q13 = worker.select("*").filter("firstName in ('Vipul', 'Satish')")
  q13.show

//  -- Q -14.Write an SQL query to print details of workers excluding first names, “Vipul”and“Satish”from the Worker table
  spark.sql("select * from worker where firstName not in ('Vipul', 'Satish')")
  val q14 = worker.select("*").filter("firstName not in ('Vipul', 'Satish')")
  q14.show

//  -- Q -15.Write an SQL query to print details of Workerswith DEPARTMENT name as“Admin”.
  spark.sql("select * from worker where department = 'Admin'")
  val q15 = worker.select("*").filter("department = 'Admin'")
  q15.show

//  -- Q -16.Write an SQL query to print details of the Workers whose FIRST_NAME contains‘a’.
  spark.sql("select * from worker where firstName like'% A %'")
  var q16 = worker.select("*").filter("firstName like'%a%'")
  q16.show

//  -- Q -17.Write an SQL query to print details of the Workers whose FIRST_NAME endswith‘a’.
  spark.sql("select * from worker where firstName like'%A'")
  var q17 = worker.select("*").filter("firstName like'%A'")

//  -- Q -18.Write an SQL query to print details of the Workers whose FIRST_NAME endswith‘h’and contains six alphabets.
//  select * from worker where firstName like'_____h';
  spark.sql("select * from worker where firstName like'_____h'")
  var q18 = worker.select("*").filter("firstName like'_____h'")
  q18.show

//  -- Q -19.Write an SQL query to print details of the Workers whose SALARY lies between 100000 and 500000.
  spark.sql("select * from worker where salary between 100000 and 500000")
  var q19 = worker.select("*").filter("salary between 100000 and 500000")

//  -- Q -20.Write an SQL query to print details of the Workers who joined in Feb 2021.
  spark.sql("select * from worker where joiningDate between '2021-02-01 00:00:00' and '2021-02-28 23:59:59'")
  var q20 = worker.select("*").filter("joiningDate between '2021-02-01 00:00:00' and '2021-02-28 23:59:59'")
  q20.show

  -- Q -21.Write an SQL query to fetch the count of employees working in the department ‘Admin’.
  (all columns must be present, Join clause)
  spark.sql("select department, COUNT(workerId) as countWorker from worker where department = 'Admin' group by department")
  worker.select(col("department"))
    .groupBy("department")
    .agg(count("workerId").as("countWorker"))
    .where("department = 'Admin'")

    -- Q -22.Write an SQL query to fetch worker nameswith salaries >= 50000 and <=100000.
  select * from worker where salary between 50000 and 100000

  -- Q -23.Write an SQL query to fetch the number of workers
  for each department in descending order.
    select department
  , COUNT(workerId) as noOfWorker from worker group by department order by noOfWorker desc;

  -- Q -24.Write an SQL query to print details of the Workers who are also Managers.
    select * from worker wrk
  inner join title ttl
    on wrk
  .workerId = ttl.workerRefId
  where workertitle in('Manager
  ');

  -- Q -25.Write an SQL query to fetch duplicate records having matching data in some fields of a table
  .
  select workerTitle
  , affectedFrom
  , count(*) from title
  group by workerTitle
  , affectedFrom
  having count (*) > 1

  Thread.sleep(1000000); //For 1000 seconds or more
  spark.stop()


}





