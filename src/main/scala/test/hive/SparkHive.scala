package test.hive

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhoucw on 下午11:19.
  */
object SparkHive {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()//.setMaster("local[2]").setAppName("test")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    sqlContext.sql("select * from tmp limit 10").show()

  }

}
