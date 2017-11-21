package test

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created on 下午5:58.
  * desc: spark 解析中文文件乱码
  * @author zhoucw
  */
object testSparkEncoding {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[3]").setAppName("test")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)


    //////////////////////////////////////////////////////////////////////////////////
    //
    // 使用textfile读取utf8格式的文件
    //  输出：
    //    安徽|合肥|0551
    //    江苏|南京|025
    //    浙江|杭州|0571
    //////////////////////////////////////////////////////////////////////////////////
    val fileUTF8 = "/tmp/input/city_utf8.txt"
    val rddUTF8 = sc.textFile(fileUTF8)
    rddUTF8.take(10).foreach(println)



    //////////////////////////////////////////////////////////////////////////////////
    //
    // 使用textfile读取GBK格式的文件
    //  输出乱码：
    //    ����|�Ϸ�|0551
    //    ����|�Ͼ�|025
    //    �㽭|����|0571
    //////////////////////////////////////////////////////////////////////////////////
    val fileGBK1 = "/tmp/input/city_gbk.txt"
    val rddGBK1 = sc.textFile(fileGBK1)
    rddGBK1.take(10).foreach(println)


    //////////////////////////////////////////////////////////////////////////////////
    //
    // 读取GBK格式乱码处理
    //  输出乱码：
    //    安徽|合肥|0551
    //    江苏|南京|025
    //    浙江|杭州|0571
    //////////////////////////////////////////////////////////////////////////////////
    val fileGBK2 = "/tmp/input/city_gbk.txt"
    val rddGBK2 = sc.hadoopFile(fileGBK2, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], 1).
      map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))
    rddGBK2.take(10).foreach(println)


  }
}
