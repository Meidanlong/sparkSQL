package meidl.jobs

import meidl.utils.CommonTables
import org.apache.spark.sql.{DataFrame, SparkSession}

object UnterminalData {

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
    var unterminal_data = spark.read.option("header", "false").option("inferSchema", false.toString).csv(path)
    unterminal_data.createOrReplaceTempView("unterminal_data")
    spark.sql("select trim(_c0) only_code, _c1 organize_code, _c2 bi_channel_name, _c8 busi_valid_date, _c4, _c5, _c6, _c7, _c11, _c20, _c12, _c14, _c16, _c15, _c10, _c17, _c19, _c18, _c13, _c9, _c21, _c22 from unterminal_data")
    unterminal_data.show(false)
    unterminal_data

    val orgCode = CommonTables
    val organizeCode = orgCode.organizeCode(spark)

    var unterminal_orgCode_data = unterminal_data.join(organizeCode, unterminal_data.col("organize_code") === organizeCode.col("RSRV_STR3"))
    unterminal_orgCode_data.createOrReplaceTempView("unterminal_orgCode_data")
    unterminal_orgCode_data.show(30)
    unterminal_orgCode_data

  }

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("UnterminalData").master("local[10]").getOrCreate()
    i020303(sparkSession, "201809")
    sparkSession.stop()
  }
}
