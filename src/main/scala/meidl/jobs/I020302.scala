import java.sql.{Connection, PreparedStatement}
import java.util.Date

import meidl.utils.DateUtils
import meidl.utils.classes.{JkRwArrears, JkRwArrears2}
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
    var jkRwArrearsInter = spark.read.option("header", "false").option("inferSchema", false).csv(path)
    jkRwArrearsInter.createOrReplaceTempView("jk_rw_arrears_inter")
    jkRwArrearsInter = spark.sql(I020302Sql.jkRwArrearsInter().toString)
    println("=======================jkRwArrearsInter=======================")
    jkRwArrearsInter.show(30)
    var i020302Unterminal = CommonTables.i020302Unterminal(spark)
    println("=======================jkRwDataUnterminal=======================")
    i020302Unterminal.show(30)
    var jkRwArrears = jkRwArrearsInter.join(i020302Unterminal, "cust_ba")
    jkRwArrears.createOrReplaceTempView("jk_rw_arrears")
    var caUnterminal = CommonTables.caUnterminal(spark)
    caUnterminal.createOrReplaceTempView("jk_rw_arrears_busi")
    CommonTables.tdChlKinddef(spark)
    CommonTables.jkChannel(spark)
    var jkRwArrearsBusiJk = spark.sql(I020302Sql.jkRwArrearsBusiJk().toString)
    println("=======================jkRwArrearsBusiJk=======================")
    jkRwArrearsBusiJk.show(false)
    jkRwArrearsBusiJk
  }

def listOfJkRwArrears(tableDF :DataFrame){
    try {
      tableDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[JkRwArrears2]

        partitionOfRecords.foreach(info => {
          var channel_id = "0"
          if(null != info.get(0) && !"".equals(info.get(0))){
            channel_id = info.get(0).toString
          }
          val ba_code = info.getAs[String]("cust_ba")
          val cycle_id = "0"
          val arrears_time = "0"
          var amount = "0";
          if(null != info.get(7) && !"".equals(info.get(7))){
             amount = info.get(7).toString
          }
          val state = "1"
          var busi_kind_id = "0"
          if(null != info.get(2) && !"".equals(info.get(2))){
            val busi_kind_id1 = info.get(2).toString
            //val busi_kind_id2 = String.valueOf(busi_kind_id1)
            busi_kind_id = busi_kind_id1.replace(".0000000000","")
            println(busi_kind_id)
          }
          var product_name = "0"
          if(null != info.get(3) && !"".equals(info.get(3))){
            product_name = info.get(3).toString
          }
          var cust_ca_code = "0"
          if(null != info.get(4) && !"".equals(info.get(4))){
            cust_ca_code = info.get(4).toString
          }
          var cust_name = "0"
          if(null != info.get(5) && !"".equals(info.get(5))){
            cust_name = info.get(5).toString
          }
          var bill_month = "0"
          if(null != info.get(6) && !"".equals(info.get(6))){
            bill_month = info.get(6).toString
          }

          list.append(JkRwArrears2(channel_id,ba_code,cycle_id ,arrears_time ,amount ,state, busi_kind_id,product_name,cust_ca_code,cust_name,bill_month))
        })

        I020302.insertOracle(list)
      })
    } catch {
      case e:Exception => e.printStackTrace()
    }

}

  def deleteDatas(): Unit ={

  }

  def insertOracle(list: ListBuffer[JkRwArrears2]) = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null
    try {
      connection = OracleUtils.getConnection()

      connection.setAutoCommit(false) //设置手动提交

      val sql = "insert into jk_rw_arrears_meidl2(channel_id,ba_code,cycle_id,arrears_time,amount,state,busi_kind_id,product_name,cust_ca_code,cust_name,bill_month) values (?,?,?,?,?,?,?,?,?,?,?)"

       pstmt = connection.prepareStatement(sql)


      for (ele <- list) {
        pstmt.setString(1, ele.channel_id)
        pstmt.setString(2, ele.ba_code)
        pstmt.setString(3, ele.cycle_id)
        pstmt.setString(4, ele.arrears_time)
        pstmt.setString(5, ele.amount)
        pstmt.setString(6, ele.state)
        pstmt.setString(7, ele.busi_kind_id)
        pstmt.setString(8, ele.product_name)
        pstmt.setString(9, ele.cust_ca_code)
        pstmt.setString(10, ele.cust_name)
        pstmt.setString(11, ele.bill_month)

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