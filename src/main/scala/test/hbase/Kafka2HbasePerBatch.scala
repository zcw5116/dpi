package test.hbase

import java.io.InputStream
import java.security.MessageDigest

import com.zyuc.common.properties.Conf
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import test.kafka.{GetProperties, SparkKafkaUtils}

/**
  * Created on 下午11:41.
  */
object Kafka2HbasePerBatch extends GetProperties {
  override def inputStreamArray: Array[InputStream] = Array(
    this.getClass.getClassLoader.getResourceAsStream("kafka.proerties")
  )

  // 获取配置文件的内容
  private val prop = props

  def main(args: Array[String]) {
    // 创建StreamingContext
    // 创建上下文
    val sparkConf = new SparkConf()//.setAppName("test").setMaster("local[3]")

    val sc = new SparkContext(sparkConf)
    //sc.getConf.set("spark.streaming.kafka.maxRatePerPartition", "2")

    val groupName = sc.getConf.get("spark.app.groupName", "tes321")

    val ssc = new StreamingContext(sc, Seconds(10))
    // 创建stream时使用的topic名字集合
    val topic = sc.getConf.get("spark.app.topic", "test1,3")  // topicName, partitionNum
    val topicName = topic.substring(0, topic.indexOf(","))
    val partitionNum = topic.substring(topic.indexOf(",") + 1).toInt

    val topics: Set[Tuple2[String, Int]] = Set(Tuple2(topicName, partitionNum))
    // zookeeper的host和ip,创建一个client
    val zkClient = new ZkClient(prop.getProperty("kafka.zookeeper.list"), Integer.MAX_VALUE, 10000, ZKStringSerializer)
    // 配置信息
    val kafkaParams = Map[String, String]("metadata.broker.list" -> prop.getProperty("kafka.metadata.broker.list"))
    //val kafkaParams = Map[String, String]("metadata.broker.list" -> prop.getProperty("kafka.metadata.broker.list"), "auto.offset.reset" -> "smallest")

    // 获取topic和partition参数
    val hTable = "mytest"

    // 获取kafkaStream
    val kafkaStream = SparkKafkaUtils.createDirectKafkaStream(ssc, kafkaParams, zkClient, topics, groupName)

    kafkaStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        val data = rdd.map(x => hbaseConvert(x, offsetRanges, groupName))

        save2Hbase(data, hTable)

        SparkKafkaUtils.saveOffsets(zkClient, groupName, rdd)
      }

    })
    ssc.start()
    ssc.awaitTermination()

  }


  def hbaseConvert(rawMsg: Tuple4[String, Int, Long, String], offsetRanges: Array[OffsetRange], groupName:String) = {
    val topic = rawMsg._1
    val partition = rawMsg._2
    val offsetRange = offsetRanges(partition)
    val from = offsetRange.fromOffset
    val cnt = offsetRange.count().toString
    val offset = rawMsg._3
    val msg = rawMsg._4
    val newMsg = msg.replaceAll("<<<!>>>", "")
    val id = topic + "_" + partition + "_" + from + "_" + offset

    val arr = newMsg.split(",", 15)
    val commandid = arr(0)
    val houseid = arr(1)
    val gathertime = arr(2)
    val srcip = arr(3)
    val destip = arr(4)
    val srcport = arr(5)
    val destport = arr(6)
    val domainname = arr(7)
    val proxytype = arr(8)
    val proxyip = arr(9)
    val proxyport = arr(10)
    val title = arr(11)
    val content = arr(12)
    val url = arr(13)
    val logid = arr(14)



    // val plainRowkey = topic + "_" + partition + "_" + from + "_" + offset
    // val rowkey = md5(plainRowkey)
    val rowkey = topic +"_" + groupName + "_" + partition + "_" + from + "_" + offset
    val put = new Put(Bytes.toBytes(rowkey))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("cnt"), Bytes.toBytes(cnt))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("offset"), Bytes.toBytes(offset))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("commandid"), Bytes.toBytes(commandid))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("houseid"), Bytes.toBytes(houseid))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("gathertime"), Bytes.toBytes(gathertime))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("srcip"), Bytes.toBytes(srcip))

    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("destip"), Bytes.toBytes(destip))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("srcport"), Bytes.toBytes(srcport))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("destport"), Bytes.toBytes(destport))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("domainname"), Bytes.toBytes(domainname))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("proxytype"), Bytes.toBytes(proxytype))

    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("proxyip"), Bytes.toBytes(proxyip))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("proxyport"), Bytes.toBytes(proxyport))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("title"), Bytes.toBytes(title))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("content"), Bytes.toBytes(content))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("url"), Bytes.toBytes(url))

    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("logid"), Bytes.toBytes(logid))

    (new ImmutableBytesWritable, put)
  }

  // save2Hbase
  def save2Hbase(data: RDD[(ImmutableBytesWritable, Put)], tableName: String) = {
    val hconf = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum", Conf.ZOOKEEPER_QUORUM)
    hconf.set("hbase.zookeeper.property.clientPort", Conf.ZOOKEEPER_CLIENTPORT)

    val jobConf = new JobConf(hconf, this.getClass)
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    //设置job的输出格式
    val job = Job.getInstance(jobConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    data.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  def esParse(rawMsg: Tuple4[String, Int, Long, String], offsetRanges: Array[OffsetRange]) = {

    try {
      val topic = rawMsg._1
      val partition = rawMsg._2
      val offsetRange = offsetRanges(partition)
      val from = offsetRange.fromOffset
      val cnt = offsetRange.count()
      val offset = rawMsg._3
      val msg = rawMsg._4

      val newMsg = msg.replaceAll("<<<!>>>", "")
      val id = topic + "_" + partition + "_" + from + "_" + offset

      val arr = newMsg.split(",", 15)
      val commandid = arr(0)
      val houseid = arr(1)
      val gathertime = arr(2)
      val srcip = arr(3)
      val destip = arr(4)
      val srcport = arr(5)
      val destport = arr(6)
      val domainname = arr(7)
      val proxytype = arr(8)
      val proxyip = arr(9)
      val proxyport = arr(10)
      val title = arr(11)
      val content = arr(12)
      val url = arr(13)
      val logid = arr(14)

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
