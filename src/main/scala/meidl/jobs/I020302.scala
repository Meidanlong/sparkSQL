import java.sql.{Connection, PreparedStatement}
import java.util.Date

import meidl.utils.DateUtils
import meidl.utils.classes.JkRwArrears
import meidl.utils.sql.I020302Sql
import meidl.utils.tables.CommonTables
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}

import scala.collection.mutable.ListBuffer

object I020302{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val jkRwArrearsInter_sql =  I020302Sql
  val commonTable = CommonTables

  def i020302(sparkSession: SparkSession, day: String): DataFrame = {
    val spark = sparkSession
    //linux下转换csv编码格式：iconv -f gb2312 -t utf8 mytext.txt -o utf8.txt
    val path = "C:\\Users\\Administrator.000\\Desktop\\I020302_" + day + ".csv"
    var jkRwArrearsInter = spark.read.option("header", "false").option("inferSchema", false.toString).csv(path)
    jkRwArrearsInter.createOrReplaceTempView("jk_rw_arrears_inter")
    jkRwArrearsInter = spark.sql(I020302Sql.jkRwArrearsInter().toString)
    jkRwArrearsInter.show(30)
    var i020302Unterminal = CommonTables.i020302Unterminal(spark)
    i020302Unterminal.show(30)
    println("=======================jkRwDataUnterminal=======================")
    var jkRwArrears = jkRwArrearsInter.join(i020302Unterminal, "cust_ba")
    jkRwArrears.createOrReplaceTempView("jk_rw_arrears")
    println("=======================jkRwArrears=======================")
    var caUnterminal = CommonTables.caUnterminal(spark)
    caUnterminal.createOrReplaceTempView("jk_rw_arrears_busi")
    CommonTables.tdChlKinddef(spark)
    CommonTables.jkChannel(spark)
    var jkRwArrearsBusiJk = spark.sql(I020302Sql.jkRwArrearsBusiJk().toString)
    jkRwArrearsBusiJk
  }

def listOfJkRwArrears(tableDF :DataFrame){
    try {
      tableDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[JkRwArrears]

        partitionOfRecords.foreach(info => {
          val id = "0"
          var channel_id = info.getAs[String]("channel_id")
          val ba_code = info.getAs[String]("cust_ba")
          val cycle_id = "0"
          val arrears_time = "0"
          var amount = info.getAs[String]("amount")
          val state = "1"
          val busi_kind_id = info.getAs[String]("busi_kind_id")
          val product_name = info.getAs[String]("product_name")
          val cust_ca_code = info.getAs[String]("cust_ca_code")
          val cust_name = info.getAs[String]("cust_name")
          var bill_month = info.getAs[String]("bill_month")

          list.append(JkRwArrears(id,channel_id,ba_code,cycle_id ,arrears_time ,amount ,state, busi_kind_id,product_name,cust_ca_code,cust_name,bill_month))
        })

        I020302.insertOracle(list)
      })
    } catch {
      case e:Exception => e.printStackTrace()
    }

}

  def insertOracle(list: ListBuffer[JkRwArrears]) = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null
    try {
      connection = OracleUtils.getConnection()

      connection.setAutoCommit(false) //设置手动提交

      //insert into jk_rw_arrears_meidl (id,channel_id ,ba_code,cycle_id ,arrears_time ,amount ,state, busi_kind_id,product_name,cust_ca_code,cust_name,bill_month) values(?,?,?,?,?,?,?,?,?,?,?,?)
      val sql = "insert into jk_rw_arrears_meidl(id,channel_id,ba_code,cycle_id,arrears_time,amount,state,busi_kind_id,product_name,cust_ca_code,cust_name,bill_month) values (?,?,?,?,?,?,?,?,?,?,?,?)"

       pstmt = connection.prepareStatement(sql)


      for (ele <- list) {
        pstmt.setString(1, ele.id)
        pstmt.setString(2, ele.channel_id)
        pstmt.setString(3, ele.ba_code)
        pstmt.setString(4, ele.cycle_id)
        pstmt.setString(5, ele.arrears_time)
        pstmt.setString(6, ele.amount)
        pstmt.setString(7, ele.state)
        pstmt.setString(8, ele.busi_kind_id)
        pstmt.setString(9, ele.product_name)
        pstmt.setString(10, ele.cust_ca_code)
        pstmt.setString(11, ele.cust_name)
        pstmt.setString(12, ele.bill_month)

        pstmt.addBatch()
      }

      pstmt.executeBatch() // 执行批量处理
      connection.commit() //手工提交
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      OracleUtils.release(connection, pstmt)
    }
  }

  def main(args: Array[String]): Unit = {
    var dataUtils = DateUtils

    val sparkSession = SparkSession.builder().appName("i020302").master("local[*]").getOrCreate()
    var beginTime :Date = new Date()
    listOfJkRwArrears(i020302(sparkSession, "meidl"))
    val endTime :Date = new Date()
    println("=======================用时："+(endTime.getTime-beginTime.getTime)/1000+"秒=======================")
    println("=======================beginTime:"+dataUtils.parseByMonth(beginTime)+"endTime:"+dataUtils.parseByMonth(endTime)+"=======================")
    sparkSession.stop()
  }
}