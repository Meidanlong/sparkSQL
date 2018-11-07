import java.sql.{Connection, DriverManager, PreparedStatement}

import meidl.utils.dbUtils.DriverUtils
import meidl.utils.tables.CommonTables.oracle

object OracleUtils{

  /**
    * 获取数据库连接
    */
  def getConnection() = {
    val oracle = DriverUtils
    var rwdTrans1o = oracle.oracleUtil()
    DriverManager.getConnection(rwdTrans1o("url"),rwdTrans1o("user"),rwdTrans1o("password"))
  }

  /**
    * 释放数据库连接等资源
    * @param connection
    * @param pstmt
    */
  def release(connection: Connection, pstmt: PreparedStatement): Unit = {
    try {
      if (pstmt != null) {
        pstmt.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }


}