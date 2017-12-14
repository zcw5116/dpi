package test

import com.zyuc.common.properties.ConfigProperties
import com.zyuc.stat.etl.utils.AccessConveterUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created on 上午12:09.
  */
object TestIDC {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[3]").setAppName("AccessLogETL")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)


    sqlContext.sql("use " + ConfigProperties.DPI_HIVE_DATABASE)

    val appName =  sc.getConf.get("spark.app.name")   //"name_201711091337" //
    val inputPath = sc.getConf.get("spark.app.inputPath", "hdfs://spark123:8020/tmp/input/IDC.13951/")

    val srcDF = sqlContext.read.format("text").load(inputPath)
    //val accSrcDF = sqlContext.createDataFrame(srcDF.map(x=>AccessConveterUtil.parse(x.getString(0))).filter(_.length !=1), AccessConveterUtil.struct)
    val idcDF = sqlContext.createDataFrame(srcDF.map(x=>TestUtil.parse(x.getString(0))).filter(_.length !=1), TestUtil.struct)
    idcDF.show(false)

  }
}
