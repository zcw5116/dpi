package test.jobserver

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import spark.jobserver.{SparkJob, SparkJobInvalid, SparkJobValid, SparkJobValidation}

import scala.util.Try

/**
  * Created by zhoucw on 上午11:38.
  */
object WordCount extends SparkJob{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("WordCountExample")
    val sc = new SparkContext(conf)
    val config = ConfigFactory.parseString("input.strings='zhou chao wei zhou chen ring chen ring ring kai', input.srcdir='2'")
    println(validate(sc, config ))
    val results = runJob(sc, config)
    println("Result is " + results)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try( (config.getString("input.strings"), config.getString("input.srcdir")) )
      .map(x=> SparkJobValid)
      .getOrElse(SparkJobInvalid("No input.string config param"))
  }


  override def runJob(sc: SparkContext, config: Config): Any = {

    val sqlContext = new HiveContext(sc)



    sqlContext.read.format("text").load("/tmp/tmp.txt").show(false)
    sc.parallelize(config.getString("input.strings").split(" ").toSeq).countByValue.foreach(println)
    println("input.srcdir: " + config.getString("input.srcdir"))
  }

}
