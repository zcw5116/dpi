package test

import com.zyuc.common.utils.DateUtils
import com.zyuc.stat.etl.utils.AccessConveterUtil.logError
import org.apache.spark.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created on 上午12:12.
  */
object TestUtil extends Logging{

  val struct = StructType(Array(
    StructField("commandid", StringType),
    StructField("houseid", StringType),
    StructField("gathertime", StringType),
    StructField("srcip", StringType),
    StructField("destip", StringType),

    StructField("srcport", StringType),
    StructField("destport", StringType),
    StructField("domainname", StringType),
    StructField("proxytype", StringType),
    StructField("proxyip", StringType),

    StructField("proxyport", StringType),
    StructField("title", StringType),
    StructField("content", StringType),
    StructField("url", StringType),
    StructField("logid", StringType)
  ))


  def parse(line:String) = {

    try{
      val newLine = line.replaceAll("<<<!>>>", "")
      val arr = newLine.split(",", 15)
      Row(arr(0), arr(1), arr(2), arr(3), arr(4), arr(5), arr(6), arr(7), arr(8), arr(9), arr(10), arr(11), arr(12), arr(13), arr(14))
    }catch {
      case e:Exception =>{
        logError(s"line $line parse error. " + e.printStackTrace())
        Row("0")
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val line = "<<<!>>>3111<<<!>>>,<<<!>>>238<<<!>>>,<<<!>>>20171112132902<<<!>>>,<<<!>>>58.223.1.112<<<!>>>,<<<!>>>202.102.92.18<<<!>>>,<<<!>>>59948<<<!>>>,<<<!>>>80<<<!>>>,<<<!>>>www.sumecjob.com<<<!>>>,<<<!>>><<<!>>>,<<<!>>><<<!>>>,<<<!>>><<<!>>>,<<<!>>><<<!>>>,<<<!>>><<<!>>>,<<<!>>>http://www.sumecjob.com/Social.aspx<<<!>>>,<<<!>>>2556928065<<<!>>>"
    val newLine = line.replaceAll("<<<!>>>", "")
    println(newLine)
  }
}
