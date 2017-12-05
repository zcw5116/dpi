package test.es

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
/**
  * Created on 下午11:14.
  */
object Kafka2ES {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[3]")
    sparkConf.set("es.nodes","spark123")
      .set("es.port","9200").set("es.index.auto.create", "true")

    val sc = new SparkContext(sparkConf)

    val sqlContext = new HiveContext(sc)

 /*   val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")
     sc.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs")
     */


/*
    // define a case class
    case class Trip(myid:String, departure: String, arrival: String)

    val upcomingTrip = Trip("id3", "OTP", "SFO")
    val lastWeekTrip = Trip("id4", "MUC", "OTP")

    val rdd = sc.makeRDD(Seq(upcomingTrip, lastWeekTrip))
    EsSpark.saveToEs(rdd, "spark/docs", Map("es.mapping.id" -> "myid"))
*/

/*    val json1 = """{"myid":"id1", "arrival" : "business", "airport" : "SFO"}"""
    val json2 = """{"myid":"id2", "participants" : 5, "arrival" : "OTP"}"""

    sc.makeRDD(Seq(json1, json2))
      .saveJsonToEs("spark/json-trips", Map("es.mapping.id" -> "myid"))*/

 /*   val game = Map("media_type"->"game","title" -> "FF VI","year" -> "1994")
    val book = Map("media_type" -> "book","title" -> "Harry Potter","year" -> "2010")
    val cd = Map("media_type" -> "music","title" -> "Surfing With The Alien")

    sc.makeRDD(Seq(game, book, cd)).saveToEs("my-collection/{media_type}")*/



/*
    val arr = List((1,"xiaoa"),(2,"xiao2"),(3,"xiao3"),(4,"xiao4"),(5,"xiao5"),(6,"xiao6"),(7,"xiao7"))
    import sqlContext.implicits._
    val rdd = sc.parallelize(arr).map(x=>(x._1, x._2)).toDF("id", "name")
    rdd.saveToEs("my-collection/test", Map("es.mapping.id" -> "id"))
*/




   /* val arr = List((1,"xiaoa"),(2,"xiao2"),(3,"xiao3"),(4,"xiao4"),(5,"xiao5"),(6,"xiao6"),(7,"xiao7"))
    val rdd = sc.parallelize(arr).map(x=>{
      Map("id"->x._1, "name"->x._2)
    })
    rdd.saveToEs("my-collection1/test", Map("es.mapping.id" -> "name"))*/


//    val arr = List((1,"xiaoa"),(2,"xiao2"),(3,"xiao3"),(4,"xiao4"),(5,"xiao5"),(6,"xiao6"),(7,"xiao7"))
//    case class Person(id:Int, name:String)
//    val rdd1 = sc.parallelize(arr).map(x=>Person(x._1, x._2))
//    rdd1.saveToEs("my-collection1/test", Map("es.mapping.id" -> "name"))

  }
}
