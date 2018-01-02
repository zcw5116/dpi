package com.zyuc.stat.etl.utils

import com.zyuc.common.utils.FileUtils
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame

import scala.reflect.io.Path

/**
  * Created by hadoop on 17-12-26.
  * @author liuzk
  */
object vpdn34ConventUtil extends  Logging{

  def convert(df:DataFrame, netType:String) : DataFrame = {
    var newDF:DataFrame = null
    if(netType == "3g"){
      newDF = df.selectExpr("IMSILTE", "ACC_PROVINCE","PCFIP","SRCIP",
        "DEVICE","NAI_SERCODE","NASPORTTYPE","IMSICDMA", "MDN","NASPORT","NASPORTID",
        "AUTH_RESULT", "case when AUTH_RESULT=1 then 1 else 0 end as auth_flag",
        "concat(substr(AUTH_TIME,3,2),substr(AUTH_TIME,6,2),substr(AUTH_TIME,9,2)) as d",
        "substr(AUTH_TIME,12,2) as h", "cast((substr(AUTH_TIME,15,2)-substr(AUTH_TIME,15,2)%5) as int) as m5")
    }
    else if(netType == "4g"){
      newDF = df.selectExpr("SRCIP","IMSILTE","NAI_SERCODE","PCFIP","MDN","IMSICDMA","NASPORT",
        "NASPORTID","DEVICE","NASPORTTYPE",
        "AUTH_RESULT", "case when AUTH_RESULT=1 then 1 else 0 end as auth_flag",
        "concat(substr(AUTH_TIME,3,2),substr(AUTH_TIME,6,2),substr(AUTH_TIME,9,2)) as d",
        "substr(AUTH_TIME,12,2) as h", "cast((substr(AUTH_TIME,15,2)-substr(AUTH_TIME,15,2)%5) as int) as m5")
    }
    else if(netType == "vpdn"){
      newDF = df.selectExpr("PDSNIP","MDN","LNSIP","DEVICE","IMSILTE","NAI_SERCODE","IMSICDMA","ENTNAME",
        "AUTH_RESULT", "case when AUTH_RESULT=1 then 1 else 0 end as auth_flag",
        "concat(substr(AUTH_TIME,3,2),substr(AUTH_TIME,6,2),substr(AUTH_TIME,9,2)) as d",
        "substr(AUTH_TIME,12,2) as h", "cast((substr(AUTH_TIME,15,2)-substr(AUTH_TIME,15,2)%5) as int) as m5")
    }
    return  newDF
  }






  def main(args: Array[String]): Unit = {

  }

}
