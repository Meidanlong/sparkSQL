package meidl.utils.dbUtils

object DriverUtils {

  def oracleUtil(): Map[String, String]= Map[String, String] {
    val rwdTrans1o = Map("url" -> "jdbc:oracle:thin:@//10.4.62.234:1521/remdb",
      "user" -> "rwd_trans1o",
      "password" -> "rwd_trans1o@rem12",
      "driver" -> "oracle.jdbc.driver.OracleDriver")
    return rwdTrans1o;
  }
}