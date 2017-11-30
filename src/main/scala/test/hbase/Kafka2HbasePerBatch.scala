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
        val data = rdd.map(x => hbaseConvert(x, offsetRanges))

        save2Hbase(data, hTable)

        SparkKafkaUtils.saveOffsets(zkClient, groupName, rdd)
      }

    })
    ssc.start()
    ssc.awaitTermination()

  }


  def hbaseConvert(rawMsg: Tuple4[String, Int, Long, String], offsetRanges: Array[OffsetRange]) = {
    val topic = rawMsg._1
    val partition = rawMsg._2
    val offsetRange = offsetRanges(partition)
    val from = offsetRange.fromOffset
    val cnt = offsetRange.count().toString
    val offset = rawMsg._3
    val msg = rawMsg._4

    // val plainRowkey = topic + "_" + partition + "_" + from + "_" + offset
    // val rowkey = md5(plainRowkey)
    val rowkey = topic + "_" + partition + "_" + from + "_" + offset
    val put = new Put(Bytes.toBytes(rowkey))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("cnt"), Bytes.toBytes(cnt))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("msg"), Bytes.toBytes(msg))
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

  def md5(s: String) = {
    MessageDigest.getInstance("MD5").digest(s.getBytes)
  }

}
