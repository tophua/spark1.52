
package ch4

import org.learningconcurrency._

/**
 * 
 * Future对象和阻塞操作,
 * Await对象result和ready都会阻塞线程
 * ready方法会阻塞调用者线程,直到指定的Future对象完善为止
 * result方法会阻塞调用者线程,但是如果Future对象已经被成功完善,那么该方法就会返回Future对象的完善值
 * 如果完善Future对象操作执行失败了,那么方法会将异常赋予Future对象.
 * 
 */

object BlockingAwait extends App {
  /**
   * 本例main线程执行了一个计算操作,该操作会检索URL规范的内容,然后进入等待状态
   */
  import scala.concurrent._
  import ExecutionContext.Implicits.global
  import scala.concurrent.duration._
  import scala.io.Source

  val urlSpecSizeFuture = Future { Source.fromURL("http://www.w3.org/Addressing/URL/url-spec.txt").size }
  //调用者线程能够等待的最长时间
  val urlSpecSize = Await.result(urlSpecSizeFuture, 10.seconds)

  log(s"url spec contains $urlSpecSize characters")

}

/**
 * 执行上下文通常是使用线程池实现,阻塞执行任务的线程会导致出现线程饥饿情况,阻塞Future计算会降低并行度,甚至导致死锁.
 */
object BlockingSleepBad extends App {
  import scala.concurrent._
  import ExecutionContext.Implicits.global
  import scala.concurrent.duration._

  val startTime = System.nanoTime
/**
 * 下面例子展示了这种情况,16个独立的Future计算调用了sleep方法,而且main线程进入了等待状态
 * 直到这些计算操作完成为止.
 */
  val futures = for (_ <- 0 until 16) yield Future {
    Thread.sleep(1000)
  }

  for (f <- futures) Await.ready(f, Duration.Inf)

  val endTime = System.nanoTime

  log(s"Total execution time of the program = ${(endTime - startTime) / 1000000} ms")
  //Runtime.getRuntime().availableProcessors()方法获得当前设备的CPU个数
  log(s"Note: there are ${Runtime.getRuntime.availableProcessors} CPUs on this machine")

}


object BlockingSleepOk extends App {
  import scala.concurrent._
  import ExecutionContext.Implicits.global
  import scala.concurrent.duration._

  val startTime = System.nanoTime
  /**
   * 如果需要在程序中使用阻塞操作,那么将执行阻塞操作的代码封装在blocking调用语句中,这可以通知执行上下文,
   * 处理任务的线程被阻塞了,并允许执行上下 文生成临时的额外线程.
   */

  val futures = for (_ <- 0 until 16) yield Future {
    /**
     * 通过使用blocking语句封装调用sleep方法的语句,当global执行上下文检测到任务的数量比处理任务线程的数量多时
     * global执行上下文就生成额外的线程.16个Future计算都能以并发方式执行,而且这段程序会在1秒之内执行完毕
     */
    blocking {//用于在执行异步操作的代码中,指明它封装的代码块中含有实现阻塞操作的代码.它本身无法实现阻塞操作
      Thread.sleep(1000)
    }
  }

  for (f <- futures) Await.ready(f, Duration.Inf)

  val endTime = System.nanoTime

  log(s"Total execution time of the program = ${(endTime - startTime) / 1000000} ms")

}

