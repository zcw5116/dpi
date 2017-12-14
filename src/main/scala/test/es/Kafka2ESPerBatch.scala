package test.es

import java.io.InputStream
import java.security.MessageDigest

import com.zyuc.common.properties.Conf
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import test.kafka.{GetProperties, SparkKafkaUtils}
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._

import scala.collection.mutable

/**
  * Created on 下午11:41.
  */
object Kafka2ESPerBatch extends GetProperties {
  override def inputStreamArray: Array[InputStream] = Array(
    this.getClass.getClassLoader.getResourceAsStream("kafka.proerties")
  )

  // 获取配置文件的内容
  private val prop = props

  def main(args: Array[String]) {
    // 创建StreamingContext
    // 创建上下文
    val sparkConf = new SparkConf()//.setAppName("test").setMaster("local[3]")
    val hostName = "DPI-ES-001"
    sparkConf.set("es.nodes",hostName)  //DPI-ES-001  spark123
      .set("es.port","9200").set("es.index.auto.create", "true")

    val sc = new SparkContext(sparkConf)
    //sc.getConf.set("spark.streaming.kafka.maxRatePerPartition", "2")

    //sc.getConf.set("es.nodes","DPI-ES-001").set("es.port","9200").set("es.index.auto.create", "true")

    val topic = sc.getConf.get("spark.app.topic", "test1,3")  // topicName, partitionNum
    val groupName = sc.getConf.get("spark.app.groupName", "tes321")
    val esIndex = sc.getConf.get("spark.app.esIndex", "mydpi33")
    val esType = sc.getConf.get("spark.app.esType", "mydoc")

    val sqlContext = new HiveContext(sc)

    val ssc = new StreamingContext(sc, Seconds(10))
    // 创建stream时使用的topic名字集合
    val topicName = topic.substring(0, topic.indexOf(","))
    val partitionNum = topic.substring(topic.indexOf(",") + 1).toInt
    val topics: Set[Tuple2[String, Int]] = Set(Tuple2(topicName, partitionNum))
    // zookeeper的host和ip,创建一个client
    val zkClient = new ZkClient(prop.getProperty("kafka.zookeeper.list"), Integer.MAX_VALUE, 10000, ZKStringSerializer)
    // 配置信息
    val kafkaParams = Map[String, String]("metadata.broker.list" -> prop.getProperty("kafka.metadata.broker.list"))
    //val kafkaParams = Map[String, String]("metadata.broker.list" -> prop.getProperty("kafka.metadata.broker.list"), "auto.offset.reset" -> "smallest")




    // 获取kafkaStream
    val kafkaStream = SparkKafkaUtils.createDirectKafkaStream(ssc, kafkaParams, zkClient, topics, groupName)


    kafkaStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        val data = rdd.map(x => esParse(x, offsetRanges, groupName)).filter(_.length !=1)//.map(x=>DPI(x(7).toString,x(8).toString,x(9).toString, x(10).toString))
        val dataDF = sqlContext.createDataFrame(data, struct)
        //data.take(10).foreach(println)
        dataDF.show
        //EsSpark.saveToEs(data, s"${esIndex}/${esType}", Map("es.mapping.id" -> "id")
        dataDF.saveToEs(s"${esIndex}/${esType}", Map("es.mapping.id" -> "id"))
        SparkKafkaUtils.saveOffsets(zkClient, groupName, rdd)
      }

    })
    ssc.start()
    ssc.awaitTermination()

  }

  case class DPI(id:String, topic:String, partition:String, from:String)


  def esParse(rawMsg: Tuple4[String, Int, Long, String], offsetRanges: Array[OffsetRange], groupName:String) = {

    try {
      val topic = rawMsg._1
      val partition = rawMsg._2
      val offsetRange = offsetRanges(partition)
      val from = offsetRange.fromOffset
      val cnt = offsetRange.count()
      val offset = rawMsg._3
      val msg = rawMsg._4

      val newMsg = msg.replaceAll("<<<!>>>", "")
      val id = topic +"_" + groupName + "_" + partition + "_" + from + "_" + offset

      val arr = newMsg.split(",", 15)

      Row(id, topic, partition, from, offset, cnt, arr(0), arr(1), arr(2), arr(3), arr(4), arr(5), arr(6), arr(7), arr(8), arr(9), arr(10), arr(11), arr(12), arr(13), arr(14))

    }catch {
      case e:Exception =>{
        Row("0")
      }
    }
  }

  val struct = StructType(Array(
    StructField("id", StringType),
    StructField("topic", StringType),
    StructField("partition", IntegerType),
    StructField("from", LongType),
    StructField("offset", LongType),

    StructField("cnt", LongType),
    StructField("commandid", StringType),
    StructField("houseid", StringType),
    StructField("gathertime", StringType),
    StructField("srcip", StringType),

    StructField("destip", StringType),
    StructField("srcport", StringType),
    StructField("destport", StringType),
    StructField("domainname", StringType),
    StructField("proxytype", StringType),

    StructField("proxyip", StringType),
    StructField("proxyport", StringType),
    StructField("title", StringType),
    StructField("content", StringType),
    StructField("url", StringType),

    StructField("logid", StringType)
  ))

  def md5(s: String) = {
    MessageDigest.getInstance("MD5").digest(s.getBytes)
  }

}
