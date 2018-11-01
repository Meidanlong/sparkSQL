import org.apache.spark.sql.{DataFrame, SparkSession}


object CommonTables{

  def organizeCode(sparkSession: SparkSession): DataFrame ={
    val spark = sparkSession
    val jdbcMap = Map("url" -> "jdbc:oracle:thin:@//10.4.62.234:1521/remdb",
      "user" -> "rwd_trans1o",
      "password" -> "rwd_trans1o@rem12",
      "dbtable" -> "rwd_core_static",
      "driver" -> "oracle.jdbc.driver.OracleDriver")
    val jdbcDF = spark.read.options(jdbcMap).format("jdbc").load
    jdbcDF.createOrReplaceTempView("rwd_core_static")
    var INTERFACE_ORGANIZE_CODE = spark.sql("select * from rwd_core_static t where t.type_id = 'INTERFACE_ORGANIZE_CODE'")
    INTERFACE_ORGANIZE_CODE.show(30,false)
    INTERFACE_ORGANIZE_CODE
  }


}