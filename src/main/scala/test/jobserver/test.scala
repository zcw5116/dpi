package test.jobserver

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import spark.jobserver._

import scala.util.Try

/**
  * Created by zhoucw on 上午11:38.
  */
object MyTest1 extends SparkHiveJob {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("WordCountExample")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    val config = ConfigFactory.parseString("sql=\"select name from default.tmp\"")
    println(validate(sqlContext, config ))
    val results = runJob(sqlContext, config)
    println("Result is " + results)
  }

  def validate(sql: HiveContext, config: Config): SparkJobValidation = SparkJobValid

  def runJob(sql: HiveContext, config: Config): Any = {
    val sc = sql.sparkContext
    val newContext = sql.newSession()
    //sql.sql("use default")
    //sc.textFile("/tmp/tmp.txt").collect().foreach(println)
    newContext.sql(config.getString("sql")).collect()
  }
}
