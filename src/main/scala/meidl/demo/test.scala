package meidl.demo

import org.apache.spark.sql.{DataFrame, SparkSession}

object test {

//  def main(args: Array[String]) {
//
//    val spark = SparkSession.builder().appName("test").master("local[3]").getOrCreate()
        //spark.
//    //spark.table("")
//
//    val path = "C:\\Users\\Admini   strator.000\\Desktop\\bbb.csv"
//    // 将json文件加载成一个dataframe
//    //val data = spark.read.format("csv").load(path)
//    var data = spark.read.option("header","false").option("inferSchema",false.toString).csv(path)
//    data.createOrReplaceTempView("jk_rw_data_unterminal")
//    data = spark.sql("select _c1 from jk_rw_data_unterminal t ")
//    data.show(false)

    /*val jdbcMap = Map("url" -> "jdbc:oracle:thin:@//10.4.62.234:1521/remdb",
      "user" -> "rwd_trans1o",
      "password" -> "rwd_trans1o@rem12",
      "dbtable" -> "rwd_core_static",
      "driver" -> "oracle.jdbc.driver.OracleDriver")
    val jdbcDF = spark.read.options(jdbcMap).format("jdbc").load
    jdbcDF.createOrReplaceTempView("rwd_core_static")
    val rwd_core_static =  spark.sql("select * from rwd_core_static")

    rwd_core_static.show(false)*/

//    val organize_code = {"name" :"北京集团客户部","code" : "JK",
//      "中心区分公司" : "CE",
//      "朝阳分公司" : "CY",
//      "朝阳区分公司" : "CY",
//      "通州分公司" : "TZ",
//      "通州区分公司" : "TZ",
//      "房山分公司" : "FS",
//      "房山区分公司" : "FS",
//      "顺义分公司" : "SY",
//      "顺义区分公司" : "SY",
//      "平谷分公司" : "PG",
//      "平谷区分公司" : "PG",
//      "密云分公司" : "MY",
//      "密云区分公司" : "MY",
//      "怀柔分公司" : "HR",
//      "怀柔区分公司" : "HR",
//      "大兴分公司" : "DX",
//      "大兴区分公司" : "DX",
//      "海淀分公司" : "CS",
//      "海淀区分公司" : "CS",
//      "昌平分公司" : "CP",
//      "昌平区分公司" : "CP",
//      "延庆分公司" : "YQ",
//      "延庆区分公司" : "YQ",
//      "西区分公司" : "CS",
//      "南区分公司" : "CE",
//      "城区一分公司" : "CY",
//      "城区二分公司" : "CE",
//      "城区三分公司" : "CS",
//      "政企客户中心" : "JK",
//      "合作拓展室" : "JK"}

//    spark.sparkContext.parallelize()
//    var format_org_code =  spark.read.format("map").options(organize_code).load()
//    format_org_code.createOrReplaceTempView("format_org_code")
//    format_org_code =spark.sql("select * from format_org_code")
//    format_org_code.show(30,false)

//    val rdd = data.toJavaRDD


//    data.map(line =>{
//
//    }).s

    //data.filter($"_c02" === "北京")
    //data.filter($"no2" === "大兴分公司")
    //val rdd = spark.sparkContext.textFile(path)

    //注意：需要导入隐式转换
    //import spark.implicits._
    //val data = rdd.map(_.split("\",\"")).map(line => I020303(line(0), line(1), line(2), line(3), line(4), line(5), line(6), line(7), line(8), line(9), line(10), line(11), line(12), line(13), line(14), line(15), line(16), line(17), line(18), line(19), line(20), line(21), line(22))).toDF()



    ///new String(dat)
    // 输出dataframe对应的schema信息
    //data.printSchema()

    // 输出数据集的前20条记录
    //data.show(100,false)
//    val num = data.select(data.col("no2")).count()
//    println("===================================================="+num)
    //val df = data.select(data.col("no2"))
    //val ds = df.filter($"no2" === "大兴分公司")
//    println("===================================================="+ds.count())

    //查询某列所有的数据： select name from table
    //    data.select("name").show()
    //
    //    // 查询某几列所有的数据，并对列进行计算： select name, age+10 as age2 from table
    //    data.select(data.col("name"), (data.col("age") + 10).as("age2")).show()
    //
    //    //根据某一列的值进行过滤： select * from table where age>19
    //    data.filter(data.col("age") > 19).show()
    //
    //    //根据某一列进行分组，然后再进行聚合操作： select age,count(1) from table group by age
    //    data.groupBy("age").count().show()

//    spark.stop()
//  }
//
//  def i020303(sparkSession: SparkSession, month: String): DataFrame = {
//    val spark = sparkSession
//    //val path = "C:\\Users\\Administrator.000\\Desktop\\I020303_" + month + ".csv"
//    val path = "C:\\Users\\Administrator.000\\Desktop\\bbb.csv"
//    //val infoDF = rdd.map(_.split(",")).map(line => Info(line(0).toInt, line(1), line(2).toInt)).toDF()
//    var unterminal_data = spark.read.option("header", "false").option("inferSchema", false.toString).csv(path)
//    //var unterminal_data = spark.sparkContext.textFile(path).map(line => I020303(line(0).toString, line(1).toString, line(2).toString, line(3).toString, line(4).toString, line(5).toString, line(6).toString, line(7).toString, line(8).toString, line(9).toString, line(10).toString, line(11).toString, line(12).toString, line(13).toString, line(14).toString, line(15).toString, line(16).toString, line(17).toString, line(18).toString, line(19).toString, line(20).toString, line(21).toString, line(22).toString)).toDF()
//    unterminal_data.createOrReplaceTempView("unterminal_data")
//    unterminal_data.show(false)
//    unterminal_data
    //unterminal_data.col("nnnnoooo2").r("organize_code")

    //    val orgCode = CommonTables
    //    val organizeCode = orgCode.organizeCode(spark)
    //
    //    var unterminal_orgCode_data = unterminal_data.join(organizeCode, unterminal_data.col("organize_code") === organizeCode.col("RSRV_STR3"))
    //    unterminal_orgCode_data.createOrReplaceTempView("unterminal_orgCode_data")
    //    unterminal_orgCode_data.show(30)
    //    unterminal_orgCode_data

//  }
//
//  case class I020303(no1: String, no2: String, no3: String, no4: String, no5: String, no6: String, no7: String, no8: String, no9: String, no10: String, no11: String, no12: String, no13: String, no14: String, no15: String, no16: String, no17: String, no18: String, no19: String, no20: String, no21: String, no22: String, no23: String)
//  case class I020330(nnnnoooo1: String, nnnnoooo2: String, nnnnoooo3: String, nnnnoooo4: String, nnnnoooo5: String, nnnnoooo6: String, nnnnoooo7: String, nnnnoooo8: String, nnnnoooo9: String, nnnnoooo10: String, nnnnoooo11: String, nnnnoooo12: String, nnnnoooo13: String, nnnnoooo14: String, nnnnoooo15: String, nnnnoooo16: String, nnnnoooo17: String, nnnnoooo18: String, nnnnoooo19: String, nnnnoooo20: String, nnnnoooo21: String, nnnnoooo22: String, nnnnoooo23: String)

}
