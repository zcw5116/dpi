package com.zyuc.stat.analyze

import com.zyuc.common.properties.ConfigProperties
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/**
  * Created on 下午11:42.
  */
object Accesslog {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[3]").setAppName("test")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    sqlContext.sql("use " + ConfigProperties.DPI_HIVE_DATABASE)

    val appName = sc.getConf.get("spark.app.name") // "name_201711041325" ////
    val acclogTable = sc.getConf.get("spark.app.table.acclogTable", "dpi_log_access_m5")
    val houseipFile = sc.getConf.get("spark.app.table.hosuripFile", "/hadoop/accesslog/basic/houseip.txt")
    val output = sc.getConf.get("spark.app.output", "/hadoop/accesslog/output/analyze/tmp/")

    val datatime = appName.substring(appName.lastIndexOf("_") + 1 )

    val d = datatime.substring(2, 8)
    val h = datatime.substring(8, 10)
    val m5 = datatime.substring(10, 12)

    import sqlContext.implicits._
    val houseSrcDF = sc.textFile(houseipFile).map(x=>x.split(",")).map(x=>(x(0),x(1))).toDF("houseid", "houseip")
    val broadvalues = sc.broadcast(houseSrcDF)

    val houseDF = broadvalues.value
    val tabHouseIP = "tabHouseIP_" + datatime
    houseDF.registerTempTable(tabHouseIP)

    val regstr = "(^[a-zA-Z0-9.-:]+$)"
    val acclogDF = sqlContext.sql(
      s"""
         |select a.houseid, srcip, destip, proctype, srcport, destport, domain,
         |       case when domain rlike '${regstr}' then 1 else 0 end as domainflag,
         |        url, duration, acctime, h.houseip
         |from ${acclogTable} a left join ${tabHouseIP} h on(a.destip = h.houseip)
         |where d='${d}' and h='${h}' and m5='${m5}'
       """.stripMargin)

    val legalDF = acclogDF.filter("houseip is not null").groupBy("houseid", "destip", "destport", "proctype").
      agg(min("acctime").alias("firsttime"), max("acctime").alias("activetime"), count(lit(1)).alias("times"))

    legalDF.repartition(10).select("firsttime", "activetime", "times").write.format("orc").mode(SaveMode.Overwrite).save(output)

  }
}
