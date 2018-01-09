package com.zyuc.common.properties

/**
  * Created by hadoop on 17-11-9.
  */
object ConfigProperties {
  val DPI_HIVE_DATABASE:String = "default"
  val DPI_ZOOKEEPER_CLIENTPORT = "2181";
  val DPI_ZOOKEEPER_QUORUM = "DPI-CDH-008,DPI-CDH-009,DPI-CDH-010"
  val IOT_HIVE_DATABASE:String = "iot"
  val IOT_ZOOKEEPER_CLIENTPORT = "2181";
  val IOT_ZOOKEEPER_QUORUM = "EPC-LOG-NM-15,EPC-LOG-NM-17,EPC-LOG-NM-16"
}
