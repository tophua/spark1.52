package demo

/**
  * Created by liush on 17-7-21.
  */
object MatchDemo extends App{
  var sign = 0
  for (ch <- "+-!") {

    ch match {
      case '+' => sign = 1
      case '-' => sign = -1
      case _ => sign = 0
    }

    println(sign)
  }

  for (ch <- "+-!") {

    sign = ch match {
      case '+' => 1
      case '-' => -1
      case _ => 0
    }
    println(sign)
  }


 /* import java.awt._

  val color = SystemColor.textText
  color match {
    case Color.RED => "Text is red"
    case Color.BLACK => "Text is black"
    case _ => "Not red or black"
  }*/


  for (ch <- "+-3!") {
    var sign = 0
    var digit = 0

    ch match {
      case '+' => sign = 1
      case '-' => sign = -1
      case _ if Character.isDigit(ch) => digit = Character.digit(ch, 10)
      case _ => sign = 0
    }

    println(ch + " " + sign + " " + digit)
  }



  val str = "+-3!"
  for (i <- str.indices) {
    var sign = 0
    var digit = 0

    str(i) match {
      case '+' => sign = 1
      case '-' => sign = -1
      case ch if Character.isDigit(ch) => digit = Character.digit(ch, 10)
      case _ =>
    }

    println(str(i) + " " + sign + " " + digit)
  }

  import scala.math._
  val x = random
  x match {
    case Pi => "It's Pi"
    case _ => "It's not Pi"
  }

  import java.io.File._
  str match {
    case `pathSeparator` => "It's the path separator"
    case _ => "It's not the path separator"
  }

  //类型匹配
  for (obj <- Array(42, "42", BigInt(42), BigInt, 42.0)) {

    val result = obj match {
      case x: Int => x
      case s: String => s.toInt
      case _: BigInt => Int.MaxValue
      case BigInt => -1
      case _ => 0
    }

    println(result)
  }
  //类型匹配
  for (obj <- Array(Map("Fred" -> 42), Map(42 -> "Fred"), Array(42), Array("Fred"))) {

    val result = obj match {
      case m: Map[String, Int] => "It's a Map[String, Int]"
      // Warning: Won't work because of type erasure
        //警告：不会匹配任何类型
      case m: Map[_, _] => "It's a map"
      case a: Array[Int] => "It's an Array[Int]"
      case a: Array[_] => "It's an array of something other than Int"
    }

    println(result)
  }
  //数组匹配
  for (arr <- Array(Array(0), Array(1, 0), Array(0, 1, 0), Array(1, 1, 0))) {

    val result = arr match {
      case Array(0) => "0"
      case Array(x, y) => x + " " + y
        //匹配第一个0
      case Array(0, _*) => "0 ..."
      case _ => "something else"
    }

    println(result)
  }

  for (lst <- Array(List(0), List(1, 0), List(0, 0, 0), List(1, 0, 0))) {

    val result = lst match {
      case 0 :: Nil => "0"
      case x :: y :: Nil => x + " " + y
      case 0 :: tail => "0 ..."
      case _ => "something else"
    }

    println(result)
  }
  //元组匹配
  for (pair <- Array((0, 1), (1, 0), (1, 1))) {
    val result = pair match {
      case (0, _) => "0 ..."
      case (y, 0) => y + " 0"
      case _ => "neither is 0"
    }

    println(result)
  }

  val pattern = "([0-9]+) ([a-z]+)".r
  //模式匹配
  "99 bottles" match {
    case pattern(num, item) => {
      println(num+"==="+item)
      (num.toInt, item)
    }
  }

  val arr = Array(1, 7, 2, 9)
  //元组取值
  val Array(first, second, _*) = arr

  import scala.collection.JavaConverters._
  // Converts Java Properties to a Scala map—just to get an interesting example
  for ((k, v) <- System.getProperties.asScala)
    println(k + " -> " + v)

  for ((k, "") <- System.getProperties.asScala)
    println(k)

  for ((k, v) <- System.getProperties.asScala if v == "")
    println(k)



  abstract class Amount
  case class Dollar(value: Double) extends Amount
  case class Currency(value: Double, unit: String) extends Amount

  case object Nothing extends Amount
  for (amt <- Array(Dollar(1000.0), Currency(1000.0, "EUR"), Nothing)) {
    val result = amt match {
      case Dollar(v) => "$" + v
      case Currency(_, u) => "Oh noes, I got " + u
      case Nothing => ""
    }
    // Note that amt is printed nicely, thanks to the generated toString
    println(amt + ": " + result)
  }
}
