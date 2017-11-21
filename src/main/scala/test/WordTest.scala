package test

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created on 上午3:01.
  */
object WordTest {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("test")

    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    val inputFile = "/tmp/tmp.txt"

    val wordRDD = sc.textFile(inputFile).flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKey(_ + _)

    wordRDD.collect().foreach(println)

  }

}
