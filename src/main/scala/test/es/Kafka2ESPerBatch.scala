package test.es

import java.io.InputStream
import java.security.MessageDigest

import com.zyuc.common.properties.Conf
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
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
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[3]")
    sparkConf.set("es.nodes","spark123")
      .set("es.port","9200").set("es.index.auto.create", "true")

    val sc = new SparkContext(sparkConf)
    //sc.getConf.set("spark.streaming.kafka.maxRatePerPartition", "2")

    val ssc = new StreamingContext(sc, Seconds(10))
    // 创建stream时使用的topic名字集合
    val topics: Set[String] = Set("test1")
    // zookeeper的host和ip,创建一个client
    val zkClient = new ZkClient(prop.getProperty("kafka.zookeeper.list"), Integer.MAX_VALUE, 10000, ZKStringSerializer)
    // 配置信息
    //val kafkaParams = Map[String, String]("metadata.broker.list" -> prop.getProperty("kafka.metadata.broker.list"))
    val kafkaParams = Map[String, String]("metadata.broker.list" -> prop.getProperty("kafka.metadata.broker.list"), "auto.offset.reset" -> "smallest")

    // 获取topic和partition参数
    val groupName = "testabc2"

    // 获取kafkaStream
    val kafkaStream = SparkKafkaUtils.createDirectKafkaStream(ssc, kafkaParams, zkClient, topics, groupName)



    kafkaStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        val data = rdd.map(x => esConvert(x, offsetRanges))
        print(data)
        data.saveToEs("my-dpi2/test", Map("es.mapping.id" -> "id"))
        SparkKafkaUtils.saveOffsets(zkClient, groupName, rdd)
      }

    })
    ssc.start()
    ssc.awaitTermination()

  }

  case class DPI(id:String, topic:String, partition:Int, from:Long, offset:Long, cnt:String, msg:String)


  def esConvert(rawMsg: Tuple4[String, Int, Long, String], offsetRanges: Array[OffsetRange]) = {


    val topic = rawMsg._1
    val partition = rawMsg._2
    val offsetRange = offsetRanges(partition)
    val from = offsetRange.fromOffset
    val cnt = offsetRange.count().toString
    val offset = rawMsg._3
    val msg = rawMsg._4
    val id = topic + "_" + partition + "_" + from + "_" + offset
    DPI(id, topic, partition, from, offset, cnt, msg)
  }



  def md5(s: String) = {
    MessageDigest.getInstance("MD5").digest(s.getBytes)
  }

}
