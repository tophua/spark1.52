package scalaDemo

import java.util.{Timer, TimerTask}

import scala.concurrent._
/**
 * Scala的Promise可以被认为一个是可写的,静态单赋值,
 * 它可以创建一个Future和完成一个Future(success complete和failed complete)promise字面意思是承诺,
 * 以为他可以控制异步操作的结果
 */
object PromiseTimedEventTestApp extends App {
  val timer = new Timer
  /** Return a Future which completes successfully with the supplied value after secs seconds. */
  def delayedSuccess[T](secs: Int, value: T): Future[T] = {
    val result = Promise[T]
    timer.schedule(new TimerTask() {
      def run() = {
        result.success(value)
      }
    }, secs * 10)
    result.future
  }
  /**
   * Return a Future which completes failing with an IllegalArgumentException after secs
   * seconds.
   */
  def delayedFailure(secs: Int, msg: String): Future[Int] = {
    val result = Promise[Int]
    timer.schedule(new TimerTask() {
      def run() = {
        result.failure(new IllegalArgumentException(msg))
      }
    }, secs * 10)
    result.future
  }
  delayedSuccess(1,timer)
  delayedFailure(2,"delayedFailure")
}
