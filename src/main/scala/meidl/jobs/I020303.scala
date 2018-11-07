package meidl.jobs

import meidl.utils.sql.I020303Sql
import meidl.utils.tables.CommonTables
import org.apache.spark.sql.{DataFrame, SparkSession}

object I020303 {
  val unterminalData_sql =  I020303Sql

  //  def before(name: String): SparkSession = {
  //    SparkSession.builder().appName(name).master("local[10]").getOrCreate()
  //  }
  //
  //  def after(): Unit = {
  //    //spark.stop()
  //  }


  def i020303(sparkSession: SparkSession, month: String): DataFrame = {
    val spark = sparkSession
    //val path = "C:\\Users\\Administrator.000\\Desktop\\I020303_" + month + ".csv"
    val path = "C:\\Users\\Administrator.000\\Desktop\\bbb.csv"
    var unterminalData = spark.read.option("header", "false").option("inferSchema", false.toString).csv(path)
    unterminalData.createOrReplaceTempView("unterminal_data")
    unterminalData = spark.sql(unterminalData_sql.unterminalData().toString)
    unterminalData.show(false)

    val orgCode = CommonTables
    val organizeCode = orgCode.organizeCode(spark)

    var unterminalOrgCodeData = unterminalData.join(organizeCode, unterminalData.col("organize_code") === organizeCode.col("RSRV_STR3"))
    unterminalOrgCodeData = unterminalOrgCodeData.drop("organize_code").drop("RSRV_STR3").withColumnRenamed("data_id","organize_code")
    unterminalOrgCodeData.createOrReplaceTempView("unterminal_orgCode_data")
    unterminalOrgCodeData.show(30)
    unterminalOrgCodeData

  }

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("i020303").master("local[*]").getOrCreate()
    i020303(sparkSession, "201809")
    sparkSession.stop()
  }
}
