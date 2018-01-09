package com.zyuc.stat.utils

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * Desc: 数据与hbase接口
  * @author zhoucw
  * @version 1.0
  */
object HbaseDataUtil {

  /**
    * Desc: 将RDD数据写入hbase表
    * @author zhoucw
    * @param htable     数据写入hbase的表名
    * @param rdd        需要保存到hbase表的RDD
    */
  def saveRddToHbase(htable: String, rdd:RDD[(ImmutableBytesWritable,Put)]):Unit={

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)
    conf.set("hbase.zookeeper.quorum", ConfigProperties.IOT_ZOOKEEPER_QUORUM)

    val jobConf = new JobConf(conf, this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, htable)

    rdd.saveAsHadoopDataset(jobConf)
  }

  def htableToDataFrame(htable:String) :DataFrame = {

    null
  }
}
