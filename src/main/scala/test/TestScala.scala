package test

import java.security.MessageDigest
import java.text.SimpleDateFormat
import java.util.Date

/**
  * Created on 下午11:34.
  */
object TestScala {
  def main(args: Array[String]): Unit = {

    println(MessageDigest.getInstance("MD5").digest("0".getBytes("ISO8859-1")))
    println(MessageDigest.getInstance("MD5").digest("1a0".getBytes))
    println(MessageDigest.getInstance("MD5").digest("1a0".getBytes("ISO8859-1")))

  }
}
