package test.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}

/**
  * Created by zhoucw on 上午10:18.
  */
object TestHbase {
  def main(args: Array[String]): Unit = {
    val tableName = "testtable"
    val family = "testFaimily"
    val rowkey = "testkey"
    val cloumn = "testc"
    val value = "testvalue"
    val conf = HBaseConfiguration.create()
    //conf.set("hbase.zookeeper.property.clientPort", ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)
    //conf.set("hbase.zookeeper.quorum", ConfigProperties.IOT_ZOOKEEPER_QUORUM)
    val conn = ConnectionFactory.createConnection(conf)
    val userTable = TableName.valueOf(tableName)
    val table = conn.getTable(userTable)
    val p = new Put(rowkey.getBytes)
    p.addColumn(family.getBytes,cloumn.getBytes, value.getBytes())
    table.put(p)
  }
}
