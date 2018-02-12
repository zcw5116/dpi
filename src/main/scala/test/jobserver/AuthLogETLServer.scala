package test.jobserver

import com.typesafe.config.{Config, ConfigFactory}
import com.zyuc.stat.etl.realtime.AuthLogETL
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import spark.jobserver.{SparkJob, SparkJobInvalid, SparkJobValid, SparkJobValidation}

import scala.util.Try

/**
  * Created by zhoucw on 上午11:38.
  */
object AuthLogETLServer extends SparkJob{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("WordCountExample")
    val sc = new SparkContext(conf)
     val config = ConfigFactory.parseString("input.datatime=\"201801111500\", inputpath=\"hdfs://spark123:8020/tmp/authlog/input/\", outputpath=\"hdfs://spark123:8020/tmp/authlog/output/\"")
    println(validate(sc, config ))
    val results = runJob(sc, config)
    println("Result is " + results)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try( (config.getString("input.datatime"), config.getString("inputpath"), config.getString("outputpath")) )
      .map(x=> SparkJobValid)
      .getOrElse(SparkJobInvalid("No input.string config param"))
  }


  override def runJob(sc: SparkContext, config: Config): Any = {

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    val sqlContext = new HiveContext(sc)
    val loadTime = config.getString("input.datatime")
    val inputPath = config.getString("inputpath")
    val outputPath = config.getString("outputpath")
/*
    println("input.datatime: " + config.getString("input.datatime"))
    println("inputpath: " + config.getString("inputpath"))
    println("outputpath: " + config.getString("outputpath"))*/

    AuthLogETL.doETL(sc, sqlContext, fileSystem, loadTime, inputPath, outputPath)


  }

}
