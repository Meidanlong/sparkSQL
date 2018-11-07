import java.util.Date

import meidl.utils.DateUtils
import meidl.utils.sql.I020302Sql
import meidl.utils.tables.CommonTables
import org.apache.spark.sql.{DataFrame, SparkSession}

object I020302{
  val jkRwArrearsInter_sql =  I020302Sql
  val commonTable = CommonTables

  def i020302(sparkSession: SparkSession, day: String): DataFrame = {
    val spark = sparkSession
    //linux下转换csv编码格式：iconv -f gb2312 -t utf8 mytext.txt -o utf8.txt
    val path = "C:\\Users\\Administrator.000\\Desktop\\I020302_" + day + ".csv"
    var jkRwArrearsInter = spark.read.option("header", "false").option("inferSchema", false.toString).csv(path)
    jkRwArrearsInter.createOrReplaceTempView("jk_rw_arrears_inter")
    jkRwArrearsInter = spark.sql(jkRwArrearsInter_sql.jkRwArrearsInter().toString)
    jkRwArrearsInter.show(30)
    var i020302Unterminal = commonTable.i020302Unterminal(spark)
    i020302Unterminal.show(30)
    println("=======================jkRwDataUnterminal=======================")
    var jkRwArrears = jkRwArrearsInter.join(i020302Unterminal, "cust_ba")
//    jkRwArrears.createOrReplaceTempView("jk_rw_arrears")
//    jkRwArrears = spark.sql("select * from jk_rw_arrears")
    jkRwArrears.show(10000)
    println("=======================jkRwArrears=======================")
    jkRwArrears

  }

  def main(args: Array[String]): Unit = {
    var dataUtils = DateUtils

    val sparkSession = SparkSession.builder().appName("i020302").master("local[*]").getOrCreate()
    var beginTime = new Date().getTime
    i020302(sparkSession, "meidl")
    val endTime = new Date().getTime
    println("=======================用时："+(endTime-beginTime)/1000+"秒=======================")
    println("======================="+beginTime.toString+"=======================")
    println("=======================beginTime:"+dataUtils.parse(beginTime.toString)+"endTime:"+dataUtils.parse(endTime.toString)+"=======================")
    sparkSession.stop()
  }
}