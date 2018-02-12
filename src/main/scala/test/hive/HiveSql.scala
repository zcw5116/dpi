package test.hive

import com.zyuc.common.properties.ConfigProperties
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * 使用spark做etl， 如果保证数据不丢失， 不重复

    使用hive实现
    首先创建一张分区表的模板
    create table template_test(name String) partitioned by (id int) stored as orc;

    每批次处理流程：
    1. 每批次创建一张临时表
    create table test_${批次的时间} like  template_test location '存储位置(带上时间)'

    比如：
    create table test_201801041135 like  template_test location '/tmp/zhou/201801041135'

    2. 将dataframe转换成表， 利用hive的动态分区插入第一步创建的临时表
    sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")  // 开启动态分区
    val df = srcDF.coalesce(2)   // 控制文件的大小（避免小文件过多）
    df.registerTempTable("test_201801041135") // 将dataframe注册成表
    sqlContext.sql("insert into tmp1 partition(id) select name, id from test_201801041135")  // 将数据插入到临时表

    3. 将第二步临时表对应的物理文件move到正式的分区下面

    临时表对应的文件如下：
    /tmp/zhou/201801041135/id=1/part-00000
    /tmp/zhou/201801041135/id=1/part-00001
    /tmp/zhou/201801041135/id=2/part-00000
    /tmp/zhou/201801041135/id=2/part-00001

    移动需要重命名文件， 文件名带上批次的时间（方便后面重新调度删除相同批次的数据）：
    正式的分区：
    /data/zhou/id=1/201801041135-1
    /data/zhou/id=1/201801041135-2
    /data/zhou/id=2/201801041135-3
    /data/zhou/id=2/201801041135-4

    刷新分区表

    4. 删除第一步创建的临时表

    5. 同一个批次的数据重新调度
    （1）、在正式分区的目录， 根据批次号（就是批次的时间， 比如上面的201801041135）删除物理文件
    （2）、重新执行上面第一步到第四步的操作
  */
object HiveSql {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[3]").setAppName("AccessLogETL")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    sqlContext.sql("use " + ConfigProperties.DPI_HIVE_DATABASE)

    val tmpTable = "tmp1"
    sqlContext.sql("drop table tmp1")
    sqlContext.sql("CREATE TABLE tmp1 like template_test location '/tmp/zhou' ")
    sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    val df = sqlContext.sql("select name, id from tmp").coalesce(1)
    df.registerTempTable("tmp2222")
    sqlContext.sql("insert into tmp1 partition(id) select name, id from tmp2222")

  }
}
