package meidl.utils.tables

import meidl.utils.dbUtils.DriverUtils
import meidl.utils.sql.{CommonTablesSql, I020302Sql}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 如果我需要一张oracle库中的表，那我应该使用sparkSession
  * 如果我要对oracle库中的表进行操作，增删改查，那我应该使用jdbc
  */
object CommonTables {
  val oracle = DriverUtils
  var rwdTrans1o = oracle.oracleUtil

  def organizeCode(sparkSession: SparkSession): DataFrame = {
    val spark = sparkSession
    rwdTrans1o += ("dbtable" -> "rwd_core_static")
    val rwdCoreStatic = spark.read.options(rwdTrans1o).format("jdbc").load
    rwdCoreStatic.createOrReplaceTempView("rwd_core_static")
    var interfaceOrganizeCode = spark.sql(CommonTablesSql.interfaceOrganizeCode.toString)
    println("=======================organizeCode=======================")
    interfaceOrganizeCode.show(30, false)
    interfaceOrganizeCode
  }

  def i020302Unterminal(sparkSession: SparkSession): DataFrame = {
    val spark = sparkSession
    rwdTrans1o += ("dbtable" -> "i020302_unterminal")
    var i020302Unterminal = spark.read.options(rwdTrans1o).format("jdbc").load
    i020302Unterminal.createOrReplaceTempView("i020302_unterminal")
    i020302Unterminal = spark.sql(CommonTablesSql.i020302Unterminal.toString)
    println("=======================i020302Unterminal=======================")
    i020302Unterminal.show(30, false)
    i020302Unterminal
  }

  def caUnterminal(sparkSession: SparkSession): DataFrame = {
    val spark = sparkSession
    rwdTrans1o += ("dbtable" -> "I020302_ca_unterminal")
    var caUnterminal = spark.read.options(rwdTrans1o).format("jdbc").load
    caUnterminal.createOrReplaceTempView("I020302_ca_unterminal")
    caUnterminal = spark.sql(I020302Sql.jkRwArrearsBusi.toString)
    println("=======================caUnterminal=======================")
    caUnterminal.show(30, false)
    caUnterminal
  }

  def tdChlKinddef(sparkSession: SparkSession): DataFrame = {
    val spark = sparkSession
    rwdTrans1o += ("dbtable" -> "td_chl_kinddef")
    var tdChlKinddef = spark.read.options(rwdTrans1o).format("jdbc").load
    tdChlKinddef.createOrReplaceTempView("td_chl_kinddef")
    tdChlKinddef = spark.sql(CommonTablesSql.tdChlKinddef.toString)
    println("=======================tdChlKinddef=======================")
    tdChlKinddef.show(30, false)
    tdChlKinddef
  }

  def jkChannel(sparkSession: SparkSession): DataFrame = {
    val spark = sparkSession
    rwdTrans1o += ("dbtable" -> "jk_channel")
    var jkChannel = spark.read.options(rwdTrans1o).format("jdbc").load
    jkChannel.createOrReplaceTempView("jk_channel")
    jkChannel = spark.sql(CommonTablesSql.jkChannel.toString)
    println("=======================tdChlKinddef=======================")
    jkChannel.show(30, false)
    jkChannel
  }

//  def jkRwArrearsMeidl(sparkSession: SparkSession): DataFrame = {
//    val spark = sparkSession
//    rwdTrans1o += ("dbtable" -> "jk_rw_arrears_meidl")
//    var jkChannel = spark.read.options(rwdTrans1o).format("jdbc").load
//    jkChannel.createOrReplaceTempView("jk_rw_arrears_meidl")
//    jkChannel = spark.sql(CommonTablesSql.jkChannel.toString)
//    jkChannel.show(30, false)
//    jkChannel
//  }

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("CommonTables").master("local[10]").getOrCreate()
    i020302Unterminal(sparkSession)
    sparkSession.close()
  }
}


