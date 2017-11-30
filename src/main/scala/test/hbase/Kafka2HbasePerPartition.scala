package test.hbase

import java.io.InputStream
import java.security.MessageDigest
import java.util

import com.zyuc.common.properties.Conf
import com.zyuc.common.utils.HbaseUtils
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import test.kafka.{GetProperties, SparkKafkaUtils}

/**
  * Created on 下午11:41.
  */
object Kafka2HbasePerPartition extends GetProperties {
  override def inputStreamArray: Array[InputStream] = Array(
    this.getClass.getClassLoader.getResourceAsStream("kafka.proerties")
  )

  // 获取配置文件的内容
  private val prop = props

  def main(args: Array[String]) {
    // 创建StreamingContext
    // 创建上下文
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[3]")

    val sc = new SparkContext(sparkConf)
    //sc.getConf.set("spark.streaming.kafka.maxRatePerPartition", "2")

    val ssc = new StreamingContext(sc, Seconds(10))
    // 创建stream时使用的topic名字集合
    val topics: Set[String] = Set("test1")
    // zookeeper的host和ip,创建一个client
    val zkClient = new ZkClient(prop.getProperty("kafka.zookeeper.list"), Integer.MAX_VALUE, 10000, ZKStringSerializer)
    // 配置信息
    val kafkaParams = Map[String, String]("metadata.broker.list" -> prop.getProperty("kafka.metadata.broker.list"))
    //val kafkaParams = Map[String, String]("metadata.broker.list" -> prop.getProperty("kafka.metadata.broker.list"), "auto.offset.reset" -> "smallest")

    // 获取topic和partition参数
    val groupName = "testabc"
    val hTable = "test"

    // 获取kafkaStream
    val kafkaStream = SparkKafkaUtils.createDirectKafkaStream(ssc, kafkaParams, zkClient, topics, groupName)

    kafkaStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {

        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        rdd.foreachPartition(p=>{
          val partitonId = TaskContext.get().partitionId()
          val pOffset = offsetRanges(partitonId)
          val topic = pOffset.topic
          val from = pOffset.fromOffset
          val until = pOffset.untilOffset
          val cnt = pOffset.count()
          val pid = pOffset.partition
          println("partitonId:" + partitonId + " topic:" + topic + " from:" + from + " until:" + until + " cnt:" + cnt + " pid:" + pid + " pOffset:" + pOffset)

          val offsetRange = offsetRanges(TaskContext.get().partitionId())
          save2Hbase(p, offsetRange, hTable)

        })





        SparkKafkaUtils.saveOffsets(zkClient, groupName, rdd)
      }

    })
    ssc.start()
    ssc.awaitTermination()

  }


  // save2Hbase
  def save2Hbase(data: Iterator[Tuple4[String, Int, Long, String]], offsetRange:OffsetRange, htable: String) = {

    val conn = HbaseUtils.getConnect(Conf.ZOOKEEPER_QUORUM, Conf.ZOOKEEPER_CLIENTPORT)
    val tableName = TableName.valueOf(htable)
    val table = conn.getTable(tableName)

    data.foreach(r => {
      val partitionId = r._2
      val topic = r._1
      val offset = r._3
      val msg = r._4

      val recordCnt = offsetRange.count()
      val from = offsetRange.fromOffset

      val rowkey = topic + "_" + partitionId + "_" + from + "_" + offset
      val put = new Put(Bytes.toBytes(rowkey))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("cnt"), Bytes.toBytes(recordCnt.toString))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("msg"), Bytes.toBytes(msg))
      table.put(put)

    })

    conn.close()
  }


  // save2Hbase
  def save2HbaseWithBuffer(data: Iterator[Tuple4[String, Int, Long, String]], offsetRange:OffsetRange, htable: String) = {

    val conn = HbaseUtils.getConnect(Conf.ZOOKEEPER_QUORUM, Conf.ZOOKEEPER_CLIENTPORT)
    val tableName = TableName.valueOf(htable)
    //val table = conn.getTable(tableName)

    val table = conn.getBufferedMutator(TableName.valueOf(htable))

    val mutations = new util.ArrayList[Mutation]()


    data.foreach(r => {
      val partitionId = r._2
      val topic = r._1
      val offset = r._3
      val msg = r._4

      val recordCnt = offsetRange.count()
      val from = offsetRange.fromOffset

      val rowkey = topic + "_" + partitionId + "_" + from + "_" + offset
      val put = new Put(Bytes.toBytes(rowkey))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("cnt"), Bytes.toBytes(recordCnt.toString))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("msg"), Bytes.toBytes(msg))
      mutations.add(put)
      table.mutate(mutations)

    })

    conn.close()
  }

  def md5(s: String) = {
    MessageDigest.getInstance("MD5").digest(s.getBytes)
  }

}
