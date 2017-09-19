package scalaDemo.threadConcurrency

import java.util.{Timer, TimerTask}

import scala.concurrent._

/** Create Future[T] instances which will be completed after a delay.
 *  建模异步事件
  */
object TimedEvent {
  //Scala 代码使用一个 java.util.Timer 来安排 java.util.TimerTask 在一个延迟之后执行
  val timer = new Timer
/**
 * delayedSuccess 函数定制了一个任务，在运行时成功完成一个 Scala Future[T]，然后将该 future 返回给调用方。
 * delayedSuccess 函数返回相同类型的 future，但使用了一个在完成 future 时发生 IllegalArgumentException 异常的失败任务
 */
  /** Return a Future which completes successfully with the supplied value after secs seconds. */
  def delayedSuccess[T](secs: Int, value: T): Future[T] = {
    val result = Promise[T]//Promise 交由任务执行者，任务执行者通过 Promise 可以标记任务完成或者失败
    //java.util.TimerTask 在一个延迟之后执行。每个 TimerTask 在运行时完成一个有关联的 future
    timer.schedule(new TimerTask() {
      def run() = {
        result.success(value)
      }
    }, secs * 1000)
    // //Future 表示一个可能还没有实际完成的异步任务的结果,针对这个结果可以添加 Callback 以便在任务执行成功或失败后做出对应的操作
    result.future
  }

  /** Return a Future which completes failing with an IllegalArgumentException after secs
    * seconds. */
  def delayedFailure(secs: Int, msg: String): Future[Int] = {
    val result = Promise[Int]//Promise 交由任务执行者,任务执行者通过 Promise 可以标记任务完成或者失败
    timer.schedule(new TimerTask() {
      def run() = {
        result.failure(new IllegalArgumentException(msg))
      }
    }, secs * 1000)
    
    //Future 表示一个可能还没有实际完成的异步任务的结果，针对这个结果可以添加 Callback 以便在任务执行成功或失败后做出对应的操作
    result.future
  }
}
