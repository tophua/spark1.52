
package ch3

import org.learningconcurrency._
import ch3._
/**
 * 原子变量
 */
object AtomicUid extends App {
  import java.util.concurrent.atomic._
  //原子变量初始化为0
  private val uid = new AtomicLong(0L)
 //incrementAndGet方法是一个复杂的可线性操作,它会同时读取变量uid值,计算该值加1得到结果,
 //将结果写回变量uid中,然后返回这个结果
  def getUniqueId(): Long = uid.incrementAndGet()

  execute {
    log(s"Got a unique id asynchronously: ${getUniqueId()}")
  }
  /**
    * ForkJoinPool-1-worker-5: Got a unique id asynchronously: 2
      main: Got a unique id: 1
    */
  log(s"Got a unique id: ${getUniqueId()}")
}


object AtomicUidCAS extends App {
  import java.util.concurrent.atomic._
  import scala.annotation.tailrec
  private val uid = new AtomicLong(0L)

  @tailrec def getUniqueId(): Long = {
    val oldUid = uid.get
    val newUid = oldUid + 1
    if (uid.compareAndSet(oldUid, newUid)) newUid
    else getUniqueId()
  }

  execute {
    log(s"Got a unique id asynchronously: $getUniqueId")
  }

  log(s"Got a unique id: $getUniqueId")
}


object AtomicLock extends App {
  import java.util.concurrent.atomic._
  private val lock = new AtomicBoolean(false)
  def mySynchronized(body: =>Unit): Unit = {
    while (!lock.compareAndSet(false, true)) {}
    try body
    finally lock.set(false)
  }

  var count = 0
  for (i <- 0 until 10) execute {
    mySynchronized { count += 1 }
  }
  Thread.sleep(1000)
  log(s"Count is: $count")
}


object AtomicStack {
  import java.util.concurrent.atomic._
  import scala.concurrent._
  import scala.annotation.tailrec

  trait Stack
  case class Node(head: Int, tail: Stack) extends Stack
  case object Bottom extends Stack

  private val stack = new AtomicReference[Stack](Bottom)
  @tailrec def push(x: Int) {
    val oldTop = stack.get()
    val newTop = Node(x, oldTop)
    if (!stack.compareAndSet(oldTop, newTop)) push(x)
  }
  @tailrec def pop(): Option[Int] = {
    stack.get() match {
      case Bottom => None
      case oldTop @ Node(head, newTop) =>
        if (stack.compareAndSet(oldTop, newTop)) Some(head)
        else pop()
    }
  }

  def main(args: Array[String]) {
    execute {
      @tailrec def poll() {
        pop() match {
          case Some(-1) =>
            log("Got -1. Done!")
          case Some(x) =>
            log(s"Got $x")
            poll()
          case None =>
            poll()
        }
      }
      poll()
    }
  
    push(1)
    push(2)
    push(3)
    Thread.sleep(100)
    push(-1)
  }

}


object AtomicArrays extends App {
  import java.util.concurrent.atomic._
  import scala.concurrent._

  private val counts = new AtomicIntegerArray(4)
  def lowerBound(): Int = {
    var cnt = 0
    for (i <- 0 until counts.length) cnt += counts.get(i)
    cnt
  }

  for (i <- 0 until counts.length) execute {
    for (_ <- 0 until 200) counts.incrementAndGet(i)
  }

  log(s"Count lower bound: ${lowerBound()}")
  log(s"Count lower bound: ${lowerBound()}")
  log(s"Count lower bound: ${lowerBound()}")
}




