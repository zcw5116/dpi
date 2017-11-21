package com.zyuc.stat.etl.realtime

import java.util.Date

import com.zyuc.common.properties.ConfigProperties
import com.zyuc.common.utils.FileUtils
import com.zyuc.stat.etl.utils.AccessConveterUtil
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.collection.mutable

/**l
  * Created on 上午3:09.
  *
  * @author zhoucw
  * @version 1.0
  */
object AccessLogETL extends Logging{
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[3]").setAppName("AccessLogETL")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)


    sqlContext.sql("use " + ConfigProperties.DPI_HIVE_DATABASE)

    val appName =  sc.getConf.get("spark.app.name")   //"name_201711091337" //
    val inputPath = sc.getConf.get("spark.app.inputPath", "hdfs://spark123:8020/tmp/input/accesslog/")
    val outputPath = sc.getConf.get("spark.app.outputPath", "hdfs://spark123:8020/tmp/output/accesslog/")
    val coalesceSize = sc.getConf.get("spark.app.coalesce.size", "1").toInt
    val accessTable = sc.getConf.get("spark.app.accessTable")

    val loadTime = appName.substring(appName.lastIndexOf("_") + 1)
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    val partitions = "d,h,m5"


    def getTemplate: String = {
      var template = ""
      val partitionArray = partitions.split(",")
      for (i <- 0 until partitionArray.length)
        template = template + "/" + partitionArray(i) + "=*"
      template
    }

    val coalesce = FileUtils.makeCoalesce(fileSystem, inputPath, coalesceSize)
    logInfo("########################coalesce#######################" + coalesce)

    val accSrc = sqlContext.read.format("text").load(inputPath).coalesce(coalesce)

    val accSrcDF = sqlContext.createDataFrame(accSrc.map(x=>AccessConveterUtil.parse(x.getString(0))).filter(_.length !=1), AccessConveterUtil.struct)

    accSrcDF.write.mode(SaveMode.Overwrite).format("orc")
      .partitionBy(partitions.split(","):_*).save(outputPath + "temp/" + loadTime)


    val outFiles = fileSystem.globStatus(new Path(outputPath + "temp/" + loadTime + getTemplate + "/*.orc"))
    val filePartitions = new mutable.HashSet[String]
    for (i <- 0 until outFiles.length) {
      val nowPath = outFiles(i).getPath.toString
      filePartitions.+=(nowPath.substring(0, nowPath.lastIndexOf("/")).replace(outputPath + "temp/" + loadTime, "").substring(1))
    }

    var begin = new Date().getTime

    FileUtils.moveTempFiles(fileSystem, outputPath, loadTime, getTemplate, filePartitions)
    logInfo("[" + appName + "] 存储用时 " + (new Date().getTime - begin) + " ms")

    begin = new Date().getTime
    filePartitions.foreach(partition => {
      var d = ""
      var h = ""
      var m5 = ""
      partition.split("/").map(x => {
        if (x.startsWith("d=")) {
          d = x.substring(2)
        }
        if (x.startsWith("h=")) {
          h = x.substring(2)
        }
        if (x.startsWith("m5=")) {
          m5 = x.substring(3)
        }
        null
      })
      if (d.nonEmpty && h.nonEmpty && m5.nonEmpty) {
        val sql = s"alter table ${accessTable} add IF NOT EXISTS partition(d='$d', h='$h',m5='$m5')"
        logInfo(s"partition $sql")
        sqlContext.sql(sql)
      }
    })

    logInfo("[" + appName + "] 刷新分区表用时 " + (new Date().getTime - begin) + " ms")

   // accSrcDF.show(200)
  }

}
