import scala.util.{Failure, Success, Try}

/**
  * Created by zhoucw on 下午2:12.
  */
object demo1 {
  def main(args: Array[String]): Unit = {
    def divideBy(x: Int, y: Int): Try[Int] = {
      Try(x / y)
    }

    println(divideBy(1, 1).getOrElse(0)) // 1
    println(divideBy(1, 0).getOrElse(0)) //0
    divideBy(1, 1).foreach(println) // 1
    divideBy(1, 0).foreach(println) // no print

    divideBy(1, 0) match {
      case Success(i) => println(s"Success, value is: $i")
      case Failure(s) => println(s"Failed, message is: $s")
    }


    println(Some("").isEmpty)

    def trimUpper(x: Option[String]): Option[String] = {
      x map (_.trim) filter (!_.isEmpty) map (_.toUpperCase)
    }

    val name1 = Some("  name  ")
    val name2 = None
    println(trimUpper(name1) ) //Some(NAME)
    println(trimUpper(name2) ) //None


    def divideBy2(x: Int, y: Int): Either[String, Int] = {
      if(y == 0) Left("Dude, can't divide by 0")
      else Right(x / y)
    }

    divideBy2(1, 0) match {
      case Left(s) => println("Answer: " + s)
      case Right(i) => println("Answer: " + i)
    }






  }
}
