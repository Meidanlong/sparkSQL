package meidl.utils.tables

import meidl.utils.dbUtils.DriverUtils
import meidl.utils.sql.CommonTablesSql
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 如果我需要一张oracle库中的表，那我应该使用sparkSession
  * 如果我要对oracle库中的表进行操作，增删改查，那我应该使用jdbc
  */
object CommonTables {
  val oracle = DriverUtils
  var rwdTrans1o = oracle.oracleUtil
  var sql = CommonTablesSql

  def organizeCode(sparkSession: SparkSession): DataFrame = {
    val spark = sparkSession
    rwdTrans1o += ("dbtable" -> "rwd_core_static")
    val rwdCoreStatic = spark.read.options(rwdTrans1o).format("jdbc").load
    rwdCoreStatic.createOrReplaceTempView("rwd_core_static")
    var interfaceOrganizeCode = spark.sql(sql.interfaceOrganizeCode.toString)
    interfaceOrganizeCode.show(30, false)
    interfaceOrganizeCode
  }

  def i020302Unterminal(sparkSession: SparkSession): DataFrame = {
    val spark = sparkSession
    rwdTrans1o += ("dbtable" -> "i020302_unterminal")
    var i020302Unterminal = spark.read.options(rwdTrans1o).format("jdbc").load
    i020302Unterminal.createOrReplaceTempView("i020302_unterminal")
    i020302Unterminal = spark.sql(sql.i020302Unterminal.toString)
    i020302Unterminal.show(30, false)
    i020302Unterminal
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("CommonTables").master("local[10]").getOrCreate()
    i020302Unterminal(sparkSession)
    sparkSession.close()
  }
}


