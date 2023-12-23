package com.sundogsoftware.spark.LearningConcepts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.to_date
object ParentSparkSQLQueriesDataFrame {
  def createDataFrames_Worker_Bonus_Title(spark : SparkSession) = {

    var workerSchema = StructType(Array(
      StructField("workerId", IntegerType, true),
      StructField("firstName", StringType, true),
      StructField("lastName", StringType, true),
      StructField("salary", DoubleType, true),
      StructField("joiningDate", TimestampType, true),
      StructField("department", StringType, true)))
    val worker = spark.read.format("csv")
      .option("header", "true")
      .schema(workerSchema)
      .option("timestampFormat", "yyyy-MM-dd HH.mm.ss")
      .load("D:/Training/Spark-Scala/data/input/Worker.csv")
    worker.show
    worker.printSchema

    var bonusSchema = StructType(Array(
      StructField("workerRefId", IntegerType, true),
      StructField("bonusAmount", DoubleType, true),
      StructField("bonusDate", DateType, true)))

    val bonus = spark.read.format("csv").option("header", "true")
      .schema(bonusSchema)
      .option("dateFormat", "yyyy-MM-dd")
      .load("D:/Training/Spark-Scala/data/input/bonus.csv")
    bonus.show
    bonus.printSchema

    var titleSchema = StructType(Array(
      StructField("workerRefId", IntegerType, true),
      StructField("workerTitle", StringType, true),
      StructField("affectedFrom", TimestampType, true)))

    val title = spark.read.format("csv").option("header", "true")
      .schema(titleSchema)
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .load("D:/Training/Spark-Scala/data/input/title.csv")
    title.show
    title.printSchema

    List(worker, bonus, title)
  }


}
