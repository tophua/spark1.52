package scalaDemo

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
/**
 * Future 表示一个可能还没有实际完成的异步任务的结果
 * scala的Future表示一个异步操作的结果状态,它持有一个值,在未来的某个之间点可用,该值是异步操作的结果,
 * 当异步操作没有完成,那么Future的isCompleted为false,当异步操作完成了且返回了值,
 * 那么Future的isCompleted返回true且是success,
 * 如果异步操作没有完成或者异常终止,那么Future的isCompleted也返回true但是是failed.
 * Future的值不知道什么时候可用,所以需要一种机制来获取异步操作的结果,
 * 一种是不停的查看Future的完成状态,另一个采用阻塞的方式,
 * scala提供了第二种方式的支持,使用scala.concurrent.Await,
 * 它有两个方法,一个是Await.ready,当Future的状态为完成时返回,一种是Await.result,直接返回Future持有的结果。
 * Future还提供了一些map,filter,foreach等操作
 */
object FutureTestApp extends App {
  val s = "Hello"
  val f: Future[String] = future {
    s + " future!"
  }
  f onSuccess {
    case msg => println(msg)
  }
  println(s) //不加这句, f onSuccess就不执行
}
