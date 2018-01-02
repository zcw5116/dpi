package com.zyuc.stat.etl.realtime

import com.zyuc.common.utils.FileUtils
import com.zyuc.stat.etl.utils.vpdn34ConventUtil
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by hadoop on 17-12-22.
  *
  * @author liuzk
  */
object vpdn34ETL {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[3]").setAppName("name_201712020501")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val appName = sc.getConf.get("spark.app.name")
    val coalesceSize = sc.getConf.get("spark.app.coalesce.size","1").toInt
    val accessTable3g = sc.getConf.get("spark.app.accessTable3g","iot_etl_data_auth_3g")
    val accessTable4g = sc.getConf.get("spark.app.accessTable4g","iot_etl_data_auth_4g")
    val accessTableVpdn = sc.getConf.get("spark.app.accessTableVpdn","iot_etl_data_auth_vpdn")
    val wild3g = sc.getConf.get("spark.app.3gwild","3g*")
    val wild4g = sc.getConf.get("spark.app.4gwild","4g*")
    val wildVpdn = sc.getConf.get("spark.app.vpdnwild","vpdn*")

    val loadTime = appName.substring(appName.lastIndexOf("_") + 1)

    val path = "hdfs://spark1234:8020/hadoop/IOT/ANALY_PLATFORM/AuthLog/"
    val inputPath = path + "srcdata/" + loadTime + "_src/"
    val dataPath = path + "srcdata/" + loadTime + "_doing/"
    val lastDataPath = path + "srcdata/" + loadTime + "_done/"
    val partitions = "d,h,m5"

    def getTemplate: String = {
      var template = ""
      val partitionArray = partitions.split(",")
      for (i <- 0 until partitionArray.length)
        template = template + "/" + partitionArray(i) + "=*"
      template
    }
    //先把文件重命名
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    FileUtils.renameHDFSDir(fileSystem,inputPath,dataPath)


    def convert(dataPathWild:String,netType:String,accessTable:String) : Unit = {
      val coalesce = FileUtils.makeCoalesce(fileSystem, dataPathWild, coalesceSize)
      val df = sqlContext.read.json(dataPathWild).coalesce(coalesce)
      val newDF = vpdn34ConventUtil.convert(df, netType)
      val outputPath = path + netType + "/"
      newDF.write.mode(SaveMode.Overwrite).format("orc")
        .partitionBy(partitions.split(","):_*)
        .save(outputPath + "temp/" + loadTime)

      val outFiles = fileSystem.globStatus(new Path(outputPath + "temp/" + loadTime + getTemplate + "/*.orc"))
      val filePartitions = new mutable.HashSet[String]
      for (i <- 0 until outFiles.length) {
        val nowPath = outFiles(i).getPath.toString
        filePartitions.+=(nowPath.substring(0, nowPath.lastIndexOf("/")).replace(outputPath + "temp/" + loadTime, "").substring(1))
      }
      FileUtils.moveTempFiles(fileSystem, outputPath, loadTime, getTemplate, filePartitions)

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
          sqlContext.sql("use iot")
          val sql = s"alter table ${accessTable} add IF NOT EXISTS partition(d='$d', h='$h',m5='$m5')"
          sqlContext.sql(sql)
        }
      })

    }

    val dataPath3g = dataPath + wild3g
    convert(dataPath3g,"3g",accessTable3g)

    val dataPath4g = dataPath + wild4g
    convert(dataPath4g,"4g",accessTable4g)

    val dataPathVpdn = dataPath + wildVpdn
    convert(dataPathVpdn,"vpdn",accessTableVpdn)

    FileUtils.renameHDFSDir(fileSystem,dataPath,lastDataPath)
  }

}
