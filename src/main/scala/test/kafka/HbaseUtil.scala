package test.kafka

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.ConnectionFactory

object HbaseUtil extends Serializable {
  private val conf = HBaseConfiguration.create()
  // private val para = Conf.hbaseConfig // Conf为配置类，获取hbase的配置
  conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "12181")
  conf.set(HConstants.ZOOKEEPER_QUORUM,"spark123")  // hosts
  private val connection = ConnectionFactory.createConnection(conf)

  def getHbaseConn: Connection = connection
}