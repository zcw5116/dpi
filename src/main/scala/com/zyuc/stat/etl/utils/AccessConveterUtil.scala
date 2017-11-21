package com.zyuc.stat.etl.utils

import com.zyuc.common.utils.DateUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.Logging

/**
  * Created on 上午3:16.
  */
object AccessConveterUtil extends Logging{



  val struct = StructType(Array(
    StructField("houseid", StringType),
    StructField("srcip", StringType),
    StructField("destip", StringType),
    StructField("proctype", StringType),
    StructField("srcport", StringType),
    StructField("destport", StringType),
    StructField("domain", StringType),
    StructField("url", StringType),
    StructField("duration", StringType),
    StructField("acctime", StringType),
    StructField("d", StringType),
    StructField("h", StringType),
    StructField("m5", StringType)
  ))

  def getTime(time:String):Tuple4[String, String, String, String] = {
    try{
      println(time)
      val timeStr = DateUtils.parseSimple(time.toLong)
      val d = timeStr.substring(2, 10).replaceAll("-", "")
      val h = timeStr.substring(11, 13)
      val m5 = timeStr.substring(14, 15) + (timeStr.substring(15, 16).toInt/5)*5
      (timeStr, d, h, m5)
    }catch {
      case e:Exception => {
       logError(s"time ${time} parse error: " + e.printStackTrace())
        ("0", "0", "0", "0")
      }
    }
  }

/*  def getTime(time:String):Tuple4[String, String, String, String] = {
    try{
      val targetfdf = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
      val timeStr = targetfdf.format(startmilis + time.toLong*1000)
      val d = timeStr.substring(2, 10).replaceAll("-", "")
      val h = timeStr.substring(11, 13)
      val m5 = timeStr.substring(14, 15) + (timeStr.substring(15, 16).toInt/5)*5
      (timeStr, d, h, m5)
    }catch {
      case e:Exception => {
        logError(s"time ${time} parse error: " + e.printStackTrace())
        ("0", "0", "0", "0")


      }
    }
  }*/

  def parse(line:String) = {

    try{
      val arr = line.split("\\|", 10)
      val timeTuple = getTime(arr(9))
      Row(arr(0), arr(1), arr(2), arr(3), arr(4), arr(5), arr(6), arr(7), arr(8), timeTuple._1,  timeTuple._2,  timeTuple._3,  timeTuple._4)
    }catch {
      case e:Exception =>{
        logError(s"line $line parse error. " + e.printStackTrace())
        Row("0")
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println(getTime("1509811472"))
  }

}
