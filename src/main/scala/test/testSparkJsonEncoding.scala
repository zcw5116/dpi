package test

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created on 下午5:58.
  * desc: spark 解析中文文件乱码
  *
  * @author zhoucw
  */
object testSparkJsonEncoding {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[3]").setAppName("test")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)


    //////////////////////////////////////////////////////////////////////////////////
    //
    // 使用spark的外部数据源读取json格式的文件, 中文乱码
    //
    //////////////////////////////////////////////////////////////////////////////////
    val jsonFile = "/tmp/input/pdsn.json"
    val jsonDF = sqlContext.read.format("json").load(jsonFile)
    jsonDF.show()


    //////////////////////////////////////////////////////////////////////////////////
    //
    // 使用spark的外部数据源读取json格式的文件, 处理中文乱码
    //
    //////////////////////////////////////////////////////////////////////////////////
    val jsonRDD = sc.hadoopFile(jsonFile,classOf[TextInputFormat],classOf[LongWritable],classOf[Text],1).map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))
    val jsonDF2 = sqlContext.read.json(jsonRDD)
    jsonDF2.show()

  }
}
