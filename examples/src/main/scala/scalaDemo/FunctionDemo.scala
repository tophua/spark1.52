package scalaDemo

/**
  * Created by liush on 17-7-21.
  */
object FunctionDemo extends App {
  (x: Double) => 3 * x

  val triple = (x: Double) => 3 * x

  Array(3.14, 1.42, 2.0).map(triple).foreach(println _)

  Array(3.14, 1.42, 2.0).map((x: Double) => 3 * x).foreach(println _)

  Array(3.14, 1.42, 2.0) map { (x: Double) => 3 * x }

  "Mary had a little lamb".split(" ").sortWith(_.length < _.length).foreach(println _)
  //匿名方法定义
  def runInThread(block: () => Unit) {
    new Thread {
      override def run() { block() }
    }.start()
  }


  runInThread { () => println("Hi") ; Thread.sleep(10000); println("Bye") }
  //匿名方法定义,缺省扩号
  def runInThreada(block: => Unit) {
    new Thread {
      override def run() { block }
    }.start()
  }

  runInThreada { println("Hi") ; Thread.sleep(1000); println("Bye") }
  //克里化函数
  def until(condition: => Boolean)(block: => Unit) {
    if (!condition) {
      block
      until(condition)(block)
    }
  }

  var x = 10
  until (x == 0) {
    x -= 1
    println(x)
  }

  Thread.sleep(10000)

}
