package com.zyuc.stat.etl.realtime

import com.zyuc.common.properties.ConfigProperties
import com.zyuc.common.utils.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable

/**
  * Created by hadoop on 17-12-13.
  */
object CoalesceLogETL {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[3]").setAppName("name_201712101132")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)


    val appName = sc.getConf.get("spark.app.name")//,"name_201712101132")
    val inputPath = sc.getConf.get("spark.app.inputPath", "hdfs://spark1234:8020/hadoop/IOT/data/cdr/input/pgw/")
    val outputPath = sc.getConf.get("spark.app.outputPath", "hdfs://spark1234:8020/hadoop/IOT/data/cdr/output/pgw/")
    val coalesceSize = sc.getConf.get("spark.app.coalesce.size", "200").toInt


    val loadTime = appName.substring(appName.lastIndexOf("_") + 1)
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    val dd=loadTime.substring(2,8)
    val hh=loadTime.substring(8,10)
    val mm=loadTime.substring(10,12).toInt
    val mm5=((mm/5)*5).toString
    val getTemplate = "/d=" + dd +"/h=" + hh +"/m5=" + mm5


    val outFiles = fileSystem.globStatus(new Path(outputPath + "data" + getTemplate +  "/*.orc"))
    val filePartitions = new mutable.HashSet[String]
    for (i <- 0 until outFiles.length) {
      val nowPath = outFiles(i).getPath.toString
      filePartitions.+=(nowPath.substring(0, nowPath.lastIndexOf("/")).replace(outputPath + "data","").substring(1))
    }
    CoalesceLogETL.move2TempFiles(fileSystem, outputPath, loadTime, getTemplate, filePartitions)


    val coalPath = new Path(outputPath + "temp" +  getTemplate )
    val coalPath2 = coalPath.toString
    val coalesce = FileUtils.makeCoalesce(fileSystem, coalPath2, coalesceSize)
    val accSrc = sqlContext.read.format("orc").load(coalPath2).coalesce(coalesce)
    accSrc.write.mode(SaveMode.Overwrite).format("orc").save(outputPath + "data" +getTemplate+"/")

    val dataPath = new Path(outputPath + "temp"+getTemplate+"/*.orc")
    fileSystem.globStatus(dataPath).foreach(x => fileSystem.delete(x.getPath(), false))
  }

  def move2TempFiles(fileSystem: FileSystem, outputPath: String, loadTime: String, template: String, partitions: mutable.HashSet[String]): Unit = {

    val dataPath = new Path(outputPath + "data" +  template + "/*.orc")
    val dataStatus = fileSystem.globStatus(dataPath)

    dataStatus.map(dataStat => {
      val dataLocation = dataStat.getPath().toString
      var tmpLocation = dataLocation.replace(outputPath + "data", outputPath + "temp")

      val tmpPath = new Path(tmpLocation)
      val dataPath = new Path(dataLocation)

      if (!fileSystem.exists(tmpPath.getParent)) {
        fileSystem.mkdirs(tmpPath.getParent)
      }
      fileSystem.rename(dataPath, tmpPath)
    })
  }

}
