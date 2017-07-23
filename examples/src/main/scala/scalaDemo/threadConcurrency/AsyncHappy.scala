package scalaDemo.threadConcurrency

import scala.concurrent._
import scala.concurrent.duration._
/**
 * 
 */
object AsyncHappy extends App {
  import ExecutionContext.Implicits.global
  
  // task definitions
  /**
   *  4 个任务方法中的每一个都为该任务的完成时刻使用了特定的延迟值：
   *  task1 为 1 秒，task2 为 2 秒，task3 为 3 秒，task4 重新变为 1 秒
   *  每个任务还接受一个输入值，是该输入加上任务编号作为 future 的（最终）结果值.
   *  
   *  向每个任务传递上一个任务返回的结果值（或者对于 task4，传递前两个任务结果的和）。
   *  如果中间两个任务同时执行，总的执行时间大约为 5 秒（1 秒 + （2 秒、3 秒中的最大值）+ 1 秒。     
   *  如果 task1 的输入为 1，那么结果为 2。如果该结果被传递给 task2 和 task3，那么结果将为 4 和 5。
   *  如果这两个结果的和 (9) 被作为输入传递给 task4，那么最终结果将为 13
   */
      
  def task1(input: Int) = TimedEvent.delayedSuccess(1, input + 1)
  def task2(input: Int) = TimedEvent.delayedSuccess(2, input + 2)
  def task3(input: Int) = TimedEvent.delayedSuccess(3, input + 3)
  def task4(input: Int) = TimedEvent.delayedSuccess(1, input + 4)
  
  /** Run tasks with block waits. */
  /**
   * 阻塞等待运行任务
   * 在设定好操作环境之后，是时候来查看 Scala 如何处理事件的完成情况了,
   * 协调 4 个任务的执行的最简单的方法是使用阻塞等待：主要线程等待每个任务依次完成
   */
  def runBlocking() = {
    val v1 = Await.result(task1(1), Duration.Inf)//Await 对象的 result() 方法来完成阻塞等待
    //该代码首先等待 task1 的结果，然后同时创建 task2 和 task3 future，并等待两个任务依次返回 future，最后等待 task4 的结果
    val future2 = task2(v1)
    val future3 = task3(v1)
    val v2 = Await.result(future2, Duration.Inf)//Duration.Inf 无线期等待
    val v3 = Await.result(future3, Duration.Inf)//Duration.Inf 无线期等待
    val v4 = Await.result(task4(v2 + v3), Duration.Inf)//Duration.Inf 无线期等待
    /**
     * 最后 3 行（创建和设置 result）使得该方法能够返回一个 Future[Int]。
     * 返回该 future，让此方法与我接下来展示的非阻塞形式一致，但该 future 将在该方法返回之前完成
     */
    val result = Promise[Int]
    result.success(v4)
    result.future
  }
/**
 * 使用 onSuccess() 处理事件的完成
 */
  /** Run tasks with callbacks. */

  def runOnSuccess() = {
    val result = Promise[Int]
    /**
      * onSuccess() 调用是嵌套式的，所以它们将按顺序执行（即使 future 未完全按顺序完成）,代码比较容易理解，但很冗长
      */
    task1(1).onSuccess(v => v match {
      case v1 => {
        val a = task2(v1)
        val b = task3(v1)
        a.onSuccess(v => v match {
          case v2 =>
            b.onSuccess(v => v match {
              case v3 => task4(v2 + v3).onSuccess(v4 => v4 match {
                case x => result.success(x)
              })
            })
        })
      }
    })
    result.future
  }
/**
 * 使用 flatMap() 方法处理这种情况的更简单的方法
 *  flatMap() 方法从每个 future 中提取单一结果值。使用 flatMap() 消除了 清单 4 中所需的 match / case 结构，
 *  提供了一种更简洁的格式，但采用了同样的逐步执行路线
 */
  /** Run tasks with flatMap. */
  def runFlatMap() = {
    task1(1) flatMap {v1 =>
      val a = task2(v1)
      val b = task3(v1)
      a flatMap { v2 =>
        b flatMap { v3 => task4(v2 + v3) }}
    }
  }

  def timeComplete(f: () => Future[Int], name: String) {
    println("Starting " + name)
    val start = System.currentTimeMillis
    val result = Await.result(f(), Duration.Inf)//Duration.Inf 无线期等待,如果为负数没有等待完成
    val time = System.currentTimeMillis - start
    println(name + " returned " + result + " in " + time + " ms.")
  }

  timeComplete(runBlocking, "runBlocking")
  timeComplete(runOnSuccess, "runOnSuccess")
  timeComplete(runFlatMap, "runFlatMap")
  
  // force everything to terminate
  System.exit(0)
}
