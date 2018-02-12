package test.kafka.offset


import java.util.Properties

import kafka.utils.ZkUtils
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Scan}
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.util.Properties


/**
  * Created by zhoucw on 下午4:41.
  */
object HbaseOffsets {

  /* Returns last committed offsets for all the partitions of a given topic from HBase in
following  cases.
*/

  def getLastCommittedOffsets(TOPIC_NAME:String,GROUP_ID:String,hbaseTableName:String):Unit ={

    val hbaseConf = HBaseConfiguration.create()

    //Connect to HBase to retrieve last committed offsets
    val conn = ConnectionFactory.createConnection(hbaseConf)
    val table = conn.getTable(TableName.valueOf(hbaseTableName))
    val startRow = TOPIC_NAME + ":" + GROUP_ID + ":" + String.valueOf(System.currentTimeMillis())
    val stopRow = TOPIC_NAME + ":" + GROUP_ID + ":" + 0
    val scan = new Scan()
    println("re:" + new String(stopRow.getBytes))
    val scanner = table.getScanner(scan.setStartRow(startRow.getBytes).setStopRow(
      stopRow.getBytes).setReversed(true)) // 设置逆向扫描
    val result = scanner.next()
    var hbaseNumberOfPartitionsForTopic = 0 //Set the number of partitions discovered for a topic in HBase to 0
    if (result != null){
      //If the result from hbase scanner is not null, set number of partitions from hbase to the  number of cells
      hbaseNumberOfPartitionsForTopic = result.listCells().size()
      println(new String(result.getRow))
      println("hbaseNumberOfPartitionsForTopic:" + hbaseNumberOfPartitionsForTopic)
    }


    scanner.close()
    conn.close()
  }



  def main(args: Array[String]): Unit = {
    //val zkClientAndConnection = ZkUtils.createZkClientAndConnection(zkUrl, sessionTimeout, connectionTimeout)
    //val zkUtils = new ZkUtils(zkClientAndConnection._1, zkClientAndConnection._2, false)
     getLastCommittedOffsets("kafkablog1", "groupid-1", "stream_kafka_offsets")
  }

}
