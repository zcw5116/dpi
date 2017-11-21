package com.zyuc.common.utils

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

/**
  * Created on 上午5:54.
  */
object DateUtils {

  val simpleFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val fastFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val fdf = FastDateFormat.getInstance("yyyyMMddHHmmss")
  val begin = fdf.parse("19700101000000")
  val startmilis:Long = begin.getTime()

  def parseFast1(time:Long) = {

    println(new Date(time))
    fastFormat.format(new Date(getTime(time)))
  }

  def parseSimple(time:Long) = {
    simpleFormat.format(new Date(getTime(time)))
  }

  def getTime(time:Long): Long = {
    startmilis + time*1000
  }


  def main(args: Array[String]): Unit = {
   println(simpleFormat.format(new Date(1509811481)))
  }
}
