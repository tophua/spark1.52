package demo

/**
  * Created by liush on 17-7-14.
  */
object TrySuccessFailueTest {
  import scala.util.{Try, Success, Failure}

  def divideBy(x: Int, y: Int): Try[Int] = {
    Try(x / y)
  }
  /**
    * Scala2.10提供了Try来更优雅的实现这一功能。对于有可能抛出异常的操作。我们可以使用Try来包裹它，得到Try的子类Success或者Failure，
    * 如果计算成功，返回Success的实例，如果抛出异常，返回Failure并携带相关信息。
    * @param args
    */
  def main(args: Array[String]): Unit = {

    println(divideBy(1, 1).getOrElse(0)) // 1
    println(divideBy(1, 0).getOrElse(0)) //0
    divideBy(1, 1).foreach(println) // 1
    divideBy(1, 0).foreach(println) // no print
    divideBy(1, 0) match {
      case Success(i) => println(s"Success, value is: $i")
      case Failure(s) => println(s"Failed, message is: $s")
    } //Failed, message is: java.lang.ArithmeticException: / by zero


  }

}
