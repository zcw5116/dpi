package com.zyuc.stat.etl.realtime

import com.zyuc.common.properties.ConfigProperties
import com.zyuc.common.utils.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable

/*
  * Created by hadoop on 17-12-13.
  * @author x
  */
object CoalesceLogETL {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[3]").setAppName("name_201712101132")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)


    val appName = sc.getConf.get("spark.app.name")//,"name_201712101135")
    val inputPath = sc.getConf.get("spark.app.inputPath", "hdfs://spark1234:8020/hadoop/IOT/data/cdr/output/pgw/")
    val coalesceSize = sc.getConf.get("spark.app.coalesce.size", "200").toInt

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    val d=dataTime.substring(2,8)
    val h=dataTime.substring(8,10)
    val m5=dataTime.substring(10,12)
    val partitionPath = s"/d=$d/h=$h/m5=$m5"

    // 将data/d=$d/h=$h/m5=$m5/下文件移动到 mergeTmp/${dataTime}/ 目录下
    FileUtils.move2Temp(fileSystem, inputPath, partitionPath)

    val tmpPath = new Path(inputPath + "mergeTmp/" +  dataTime )
    val coalesce = FileUtils.makeCoalesce(fileSystem, tmpPath, coalesceSize)
    val srcDF = sqlContext.read.format("orc").load(coalPath2).coalesce(coalesce)
    srcDF.write.mode(SaveMode.Overwrite).format("orc").save(inputPath + "data" + partitionPath+"/")

    val tmpFiles = new Path(inputPath + "mergeTmp/" +  dataTime +"/*.orc")
    fileSystem.globStatus(tmpFiles).foreach(x => fileSystem.delete(x.getPath(), false))
  }


}
