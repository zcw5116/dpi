package test

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**a
  * Created by hadoop on 17-11-8.
  */
object test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[3]").setAppName("test")
    val sc = new SparkContext(sparkConf)

    val sqlContext = new HiveContext(sc)

    sqlContext.sql("use iot")

    val sql =
      s"""
         |select mdn, acce_province, acce_region, bsid, sid, nid, cellid, secid, event_time
         |from iot_cdr_data_pdsn where d='171120'
       """.stripMargin
    sqlContext.sql(sql).coalesce(50).write.format("orc").mode(SaveMode.Append).save("/tmp/zhoucdr/")

    sqlContext.read.format("orc").load("/tmp/zhoucdr/").coalesce(20).write.format("orc").mode(SaveMode.Append).save("/tmp/zhoucdr1/")


    import sqlContext.implicits._
    val user = sqlContext.read.format("text").load("/tmp/testuser/tmpuser.txt").
      map(x=>x.getString(0).split("\t", 3)).map(x=>(x(0), x(1), x(2))).toDF("id", "type", "mdn")
    user.registerTempTable("user")

    val pdsn = sqlContext.read.format("orc").load("/tmp/zhoucdr1/*.orc").registerTempTable("pdsn")

    val resultSQL =
      s"""
         |select mdn, id, type, acce_province, acce_region, bsid, sid, nid, cellid, secid, event_time
         |from (
         |select mdn, id, type, acce_province, acce_region, bsid, sid, nid, cellid, secid, event_time,
         |row_number() over(partition by mdn order by event_time desc) rn
         |from (
         |select u.mdn, u.id, u.type, p.mdn as mdn2,  acce_province, acce_region, bsid, sid, nid, cellid, secid, event_time
         |from user u left join pdsn p on(u.mdn=p.mdn)
         |) p
         | ) t where rn = 1 order by type, cast(id as int)
       """.stripMargin

    val resultDF = sqlContext.sql(resultSQL)

    resultDF.write.format("orc").mode(SaveMode.Overwrite).save("/tmp/tmp/")

    sqlContext.sql("select count(*) from tmp").show()
    val srcDF = sqlContext.read.format("text").load("/tmp/input/README.txt")
    srcDF.rdd.flatMap(x=>x.getString(0).split(",")).map(x=>(x,1)).reduceByKey(_ + _).collect().foreach(println)

    println("scala hello")

    val fcuser = sqlContext.read.format("text").load("/tmp/fcuser.txt").
      map(x=>x.getString(0).split("\t", 2)).map(x=>(x(0), x(1))).toDF("id", "mdn")

    val bdf = sc.broadcast(fcuser)
    val gdf = bdf.value
    gdf.registerTempTable("fcuser")

    val iotuser = sqlContext.read.format("orc").load("/tmp/d=20170922/*.orc").registerTempTable("iotuser")

    val usersql =
      s"""select mdn,'' as imsicdma,'' as imsilte,'' as iccid,'' as imei,'P100003475' as companycode,'' as vpdncompanycode,'' as apncompanycode,
         |'' as nettype,'' as vpdndomain,'0' as isvpdn,'0' as isdirect,'' as subscribetimeaaa,'' as subscribetimehlr,
         |'' as subscribetimehss,'' as subscribetimepcrf,'' as firstactivetime,'' as userstatus,'' as atrbprovince,
         |'' as userprovince,'' as belo_city,'' as belo_prov,'' as custstatus,'' as custtype,'' as prodtype,'' as internettype,
         |'0' as vpdnonly,'1' as iscommon from (
         |select f.mdn, u.mdn as umdn from fcuser f left join iotuser u on (u.mdn = f.mdn)
         |) t where t.umdn is null
       """.stripMargin

    val res = sqlContext.sql(usersql)

    res.write.format("orc").mode(SaveMode.Overwrite).save("/tmp/zhou2/")

    sqlContext.read.format("orc").load("/tmp/zhou2/*.orc").coalesce(1).write.format("orc").mode(SaveMode.Overwrite).save("/tmp/zhou3/")

  }
}
