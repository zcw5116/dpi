package test.hbase

import java.io.InputStream

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
import test.kafka.{GetProperties, HbaseUtil, SparkKafkaUtils}

/**
  * Created on 下午11:41.
  */
object Kafka2HbaseConsumer extends GetProperties {
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
    val topics: Set[Tuple2[String, Int]] = Set(Tuple2("test1",3))
    // zookeeper的host和ip,创建一个client
    val zkClient = new ZkClient(prop.getProperty("kafka.zookeeper.list"),Integer.MAX_VALUE,10000, ZKStringSerializer)
    // 配置信息
    val kafkaParams = Map[String, String]("metadata.broker.list" -> prop.getProperty("kafka.metadata.broker.list"))
    //val kafkaParams = Map[String, String]("metadata.broker.list" -> prop.getProperty("kafka.metadata.broker.list"), "auto.offset.reset" -> "smallest")

    // 获取topic和partition参数
    val groupName = "testabc"

    // 获取kafkaStream
    val kafkaStream = SparkKafkaUtils.createDirectKafkaStream(ssc, kafkaParams, zkClient, topics, groupName)

    kafkaStream.foreachRDD(rdd => {
      if(!rdd.isEmpty()){
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

            //save2Hbase(p, "test")

        })

        SparkKafkaUtils.saveOffsets(zkClient, groupName, rdd)
      }

    })
    ssc.start()
    ssc.awaitTermination()

  }



}
