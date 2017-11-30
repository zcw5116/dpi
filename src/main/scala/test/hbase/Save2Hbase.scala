package test.hbase

import com.zyuc.common.properties.Conf
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created on 下午10:43.
  */
object Save2Hbase {
  def main(args: Array[String]): Unit = {
    if(args.length < 1) {
      System.err.println("Usage: SaveData <input file>")
      System.exit(1)
    }
    val sconf = new SparkConf().setAppName("SaveRDDToHBase")
    val sc = new SparkContext(sconf)
    //载入RDD
    val input = sc.objectFile[((String,Int),Seq[(String,Int)])](args(0))
    //创建HBase配置
    val hconf = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum",Conf.ZOOKEEPER_QUORUM)
    hconf.set("hbase.zookeeper.property.clientPort",Conf.ZOOKEEPER_CLIENTPORT)
    //创建JobConf，设置输出格式和表名
    val jobConf = new JobConf(hconf, this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    val tableName = "wanglei:" + args(0).split('/')(1)
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,tableName)
    //将RDD转换成RRD[(ImmutableBytesWritable, Put)]类型
    val data = input.map{case(k,v) => convert(k,v)}
    //保存到HBase表
    data.saveAsHadoopDataset(jobConf)
    sc.stop()
  }
  //RDD转换函数
  def convert(k:(String,Int), v: Iterable[(String, Int)]) = {
    val rowkey = (k._1).reverse + k._2
    val put = new Put(Bytes.toBytes(rowkey))
    val iter = v.iterator
    while(iter.hasNext) {
      val pair = iter.next()
      put.addColumn(Bytes.toBytes("labels"), Bytes.toBytes(pair._1), Bytes.toBytes(pair._2))
    }
    (new ImmutableBytesWritable, put)
  }
}
