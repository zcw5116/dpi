package test.hbase

import com.zyuc.common.properties.Conf
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created on 下午10:43.
  */
object Save2HbaseNewApi {
  def main(args: Array[String]): Unit = {
    val sconf = new SparkConf().setAppName("SaveRDDToHBase").setMaster("local[3]")
    val sc = new SparkContext(sconf)
    //val input = sc.objectFile[((String,Int),Seq[(String,Int)])](args(0))\

    val input = sc.textFile("/tmp/abc.txt")

    val hconf = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum",Conf.ZOOKEEPER_QUORUM)
    hconf.set("hbase.zookeeper.property.clientPort",Conf.ZOOKEEPER_CLIENTPORT)
    val tableName = "test"
    val data = input.map(x => myConvert(x))
    //保存到HBase表
    save2Hbase(data, tableName)

    sc.stop()
  }

  def myConvert(line:String) = {
    val lineArr = line.split("\\|", 3)
    val prov = lineArr(0)
    val city = lineArr(1)
    val code = lineArr(2)
    val rowkey = prov + "_" + city

    val put = new Put(Bytes.toBytes(rowkey))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("prov"), Bytes.toBytes(prov))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("city"), Bytes.toBytes(city))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("code"), Bytes.toBytes(code))
    (new ImmutableBytesWritable, put)
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

  // save2Hbase
  def save2Hbase(data:RDD[(ImmutableBytesWritable, Put)], tableName:String) = {
    val hconf = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum",Conf.ZOOKEEPER_QUORUM)
    hconf.set("hbase.zookeeper.property.clientPort",Conf.ZOOKEEPER_CLIENTPORT)

    val jobConf = new JobConf(hconf, this.getClass)
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,tableName)
    //设置job的输出格式
    val job = Job.getInstance(jobConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    data.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}
