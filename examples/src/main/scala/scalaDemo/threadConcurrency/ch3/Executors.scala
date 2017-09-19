
package ch3

import org.learningconcurrency._
import ch3._
import java.util.concurrent.TimeUnit
/**
 * 实例化ForkJoinPool接口方式,以及对其提交能够通过异步方式处理的任务的方式
 */
object ExecutorsCreate extends App {
  import scala.concurrent._
  import java.util.concurrent.TimeUnit
  /**
   * Fork/Join框架是Java7提供了的一个用于并行执行任务的框架， 是一个把大任务分割成若干个小任务，
   * 最终汇总每个小任务结果后得到大任务结果的框架
   * 
   * ForkJoinPool:ForkJoinTask需要通过ForkJoinPool来执行，任务分割出的子任务会添加到当前工作线程所维护的双端队列中，进入队列的头部。
   * 当一个工作线程的队列里暂时没有任务时，它会随机从其他工作线程的队列的尾部获取一个任务
   **/
  val executor = new java.util.concurrent.ForkJoinPool //实例化ForkJoinPool类
  /**
   * ForkJoinPool接口execute接收Runnable
   */
  executor.execute(new Runnable {
    def run() = log("This task is run asynchronously.")
  })
  //
   //Thread.sleep(500)
  //不使用Thread.sleep,设置完成所有任务所需的最长等待时间,等待60SECONDS,主线程才结束
   executor.awaitTermination(60, TimeUnit.SECONDS)
}
/**
 * ExecutionContext类的伴生对象含有默认的上下文,该对象的内部使用ForkJoinPool实例
 */

object ExecutionContextGlobal extends App {
  import scala.concurrent._
  val ectx = ExecutionContext.global
  ectx.execute(new Runnable {
    def run() = log("Running on the execution context.")
  })
  Thread.sleep(1000)
  //不使用Thread.sleep,设置完成所有任务所需的最长等待时间
   
}
/**
 * ExecutionContext类的伴生对象定义两个方法ExecutionContextExecutor和ExecutionContextExecutorService
 * 该例子通过new forkjoin.ForkJoinPool创建ExecutionContextExecutor对象,意味着这个ForkJoinPool实例通常会
 * 在其他线程池中保持默认线程数,实例全局对象ExecutionContext代码更加简洁.
 * 
 */

object ExecutionContextCreate extends App {
  import scala.concurrent._
  val ectx = ExecutionContext.fromExecutorService(new forkjoin.ForkJoinPool)
  ectx.execute(new Runnable {
    def run() = log("Running on the execution context again.")
  })

}
/**
 * 声明32个独立操作,每个操作会持续两秒钟,并执行完成之前拥有10秒的等待时间
 */

object ExecutionContextSleep extends App {
  import scala.concurrent._
  for (i <- 0 until 32) execute {
    Thread.sleep(2000)
    log(s"Task $i completed.")
  }
  Thread.sleep(10000)
}


