import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SQLContext

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
    var i020303 = spark.read.option("header", "false").option("inferSchema", false.toString).csv(path)
    i020303.createOrReplaceTempView("i020303")
    i020303 = spark.sql("select _c1 organize_code from i020303 t ")
    //i020303.col("_c1").as("RSRV_STR3")
    i020303.show()

    val orgCode = CommonTables
    val organizeCode = orgCode.organizeCode(spark)

    var data =i020303.join(organizeCode,i020303.col("organize_code") ===  organizeCode.col("RSRV_STR3"))
    data.show(30)
    data

  }

  def i020305(date: String): DataFrame = {
    val spark = SparkSession.builder().appName("i020305").master("local[3]").getOrCreate()
    val path = "C:\\Users\\Administrator.000\\Desktop\\I020303_" + date + ".csv"
    spark.read.option("header", "false").option("inferSchema", false.toString).csv(path)


  }

  def logic(i020303: DataFrame, i020305: DataFrame): DataFrame = {
    i020303

  }

  case class I020303(no1: String, no2: String, no3: String, no4: String, no5: String, no6: String, no7: String, no8: String, no9: String, no10: String, no11: String, no12: String, no13: String, no14: String, no15: String, no16: String, no17: String, no18: String, no19: String, no20: String, no21: String, no22: String, no23: String)

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("UnterminalData").master("local[10]").getOrCreate()
    i020303(sparkSession,"201809")
    sparkSession.stop()
  }
}