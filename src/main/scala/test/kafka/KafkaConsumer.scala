package test.kafka

import java.io.InputStream

import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Try

/**
  * Created on 下午11:41.
  */
object KafkaConsumer extends GetProperties {
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
        println("offsetRanges(0)" + offsetRanges(0))

        rdd.foreachPartition(p=>{
          val connection = HbaseUtil.getHbaseConn
          println("partition:" + TaskContext.get().partitionId() + " num:1 size:" + p.size )
          println("partition:" + TaskContext.get().partitionId() + " num:2 size:" + p.size )
          println("partition:" + TaskContext.get().partitionId() + " num:2 hasnext:" + p.hasNext )


          p.foreach(x=>{

      /*      val tableName = TableName.valueOf("test")
            val t = connection.getTable(tableName)
            val partitionId = TaskContext.get.partitionId()
            val put = new Put(Bytes.toBytes(x._2 + "_" + "testkey_" + partitionId + "_" + i)) // row key
            put.addColumn("cf1".getBytes, "cc".getBytes,  x._2.getBytes)
            put.addColumn("cf1".getBytes, "len".getBytes, plen.toString.getBytes)
            i = i + 1
            t.put(put)*/

 /*           val tableName = TableName.valueOf("test")
            val t = connection.getTable(tableName)
            try {

              val partitionId = TaskContext.get.partitionId()

              val put = new Put(Bytes.toBytes(x._2 + "_" + "testkey_" + partitionId + "_" + i)) // row key
              // column, qualifier, value
              put.addColumn("cf1".getBytes, "cc".getBytes,  x._2.getBytes)
              i = i + 1
              put.addColumn("cf1".getBytes, "len".getBytes, p.size.toString.getBytes)
              t.put(put)
              // do some log（显示在worker上）
            } catch {
              case e: Exception =>
                // log error
                e.printStackTrace()
            } finally {
              t.close()
            }*/
            println(x)
          })
        })

        SparkKafkaUtils.saveOffsets(zkClient, groupName, rdd)
      }

    })
    ssc.start()
    ssc.awaitTermination()

  }

}
