package test.oracle

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created on 下午7:09.
  */
object TestOracle {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val jdbcDF = sqlContext.read.format("jdbc").options(
      Map("url" -> "jdbc:oracle:thin:slview/slview@//192.168.6.16:1521/orabi",
        "dbtable" -> "WSEXTDEVICE", "driver" -> "oracle.jdbc.driver.OracleDriver")).load()
    jdbcDF.show()

  }

 def getOracleTable() = {

 }
}
