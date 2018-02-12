package test.kafka

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.{Logging, SparkConf, SparkContext, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import test.KafkaSink

/**
  * Created on 上午3:04.
  */
object StreamingDemo extends Logging{
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[3]").setAppName("test")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val ssc = new StreamingContext(sc, Seconds(9))
    val lineStream = ssc.textFileStream("/tmp/tmp")

    import sqlContext.implicits._

     def  getOracleTable() = {
       val tdf = sqlContext.read.format("jdbc").options(
          Map("url" -> "jdbc:oracle:thin:slview/slview@//192.168.6.16:1521/orabi",
            "dbtable" -> "bgpprefix", "driver" -> "oracle.jdbc.driver.OracleDriver")).load().rdd.map(x=>(x.getString(1), x.getString(2))).collect()

       val odf = sc.parallelize(tdf).map(x=>(x._1, x._2)).toDF("id", "name")



       odf
    }

    // 广播KafkaSink
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", Conf.brokers)
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }
      log.warn("kafka producer init done!")
      ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }
    // 定义
   // var yourBroadcast = BroadcastWrapper[DataFrame](ssc, getOracleTable)

    println("test#########################################")
    println("test#########################################")

    val fmt = new SimpleDateFormat("mm")
    val t = fmt.format(new Date(System.currentTimeMillis)).toInt
    //输出到kafka
    lineStream.transform(rdd=>{
      //yourBroadcast.update(getOracleTable, true)
      //val df = yourBroadcast.value
      //df.show()
      rdd
    }).foreachRDD(rdd => {
      if (!rdd.isEmpty) {
        val words = rdd.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _).map(x=>x._1 + "|" + x._2)
        words.collect().foreach(println)
        words.foreach(record => {
          println("record:" + record)
          kafkaProducer.value.send(Conf.outTopics, record)
          // do something else
        })
      }
    })


    ssc.start()
    ssc.awaitTermination()
  }
}
