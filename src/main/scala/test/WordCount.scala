package test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * Created on 上午3:08.
  */
object WordCount {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("test")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val sQLContext2 = new HiveContext(sc)

    sQLContext2.read.format("text").load("/tmp/tmp.txt").show()

    val filePath = "/tmp/tmp.txt"

    doWordCount(sc, sqlContext, filePath)

  }


  def doWordCount(sc: SparkContext, parentContext: SQLContext, filePath: String) = {

    val sqlContext = parentContext.newSession()
    val fileRDD = sc.textFile(filePath).flatMap(x => x.split(" ")).map(x => (x, 1)).reduceByKey(_ + _).map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1))
    fileRDD.collect().foreach(println)

  }
}
