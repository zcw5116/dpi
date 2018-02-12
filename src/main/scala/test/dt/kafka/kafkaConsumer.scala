package test.dt.kafka

/**
  * Created by zhoucw on 下午1:42.
  */
import kafka.common.TopicAndPartition
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.{KafkaUtils, OffsetRange}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by yuhui on 2016/11/17.
  */

object kafkaConsumer extends Serializable{

  val topicsSet = Set("test1")

  val filePath = "/tmp/testkafkalog"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("App_Name").setMaster("local[4]").set("sp‌​ark.driver.port", "180‌​80");
    val sc = new SparkContext(conf)
    val blockSize = 1024 * 1024 * 128 // 128MB
    sc.hadoopConfiguration.setInt("dfs.blocksize", blockSize)

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "spark123:9092",
      "auto.offset.reset" -> "smallest"
    )

    val kafkaHelper = new KafkaClusterHelper(kafkaParams)

    var num: Long = 0

    try {
      //获取Zookeeper中最新的offset,如果第一次则取kafkaParams中的smallest
      val offsets = ZookeeperHelper.loadOffsets(topicsSet, kafkaHelper.getFromOffsets(kafkaParams, topicsSet))

      //获取kafka中最新的offset
      val latestOffsets = KafkaClusterHelper.checkErrors(kafkaHelper.getLatestLeaderOffsets(offsets.keySet))

      val offsetRanges = offsets.keys.map { tp =>
        val fromOffset = offsets(tp)
        val latestOffset = latestOffsets(tp).offset

        println("topicName和partition====>"+tp+ "  fromOffset====>"+fromOffset+"  latestOffset====>"+latestOffset)

        //OffsetRange(tp, 8800000, Math.min(fromOffset + 1024 * 1024, latestOffset)) //限制成大约是500M
        OffsetRange(tp, fromOffset, latestOffset) //限制成大约是500M

      }.toArray

      val rdd = KafkaUtils.createRDD[String, String, StringDecoder, StringDecoder](sc, kafkaParams, offsetRanges)
      println("rdd.count()====================》"+rdd.count())

      //rdd存在本地
      rdd.map(line=>{val lenth = line.toString().substring(0,line.toString().length-1)}).coalesce(1,true).saveAsTextFile(filePath)

      val nextOffsets = offsetRanges.map(x => (TopicAndPartition(x.topic, x.partition), x.untilOffset)).toMap
      //将offset存储到zookeeper，zookeeper存储路径可以删除，保证数据不丢失及数据重新读入
      ZookeeperHelper.storeOffsets(nextOffsets)

    }

  }

}