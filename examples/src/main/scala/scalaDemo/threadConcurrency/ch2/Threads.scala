package ch2

import org.learningconcurrency._
import ch2._

object ThreadsMain extends App {
  //获取当前线程的名称
  val name = Thread.currentThread.getName
  //currentThread获得当前线程对象的引用
  val t:Thread = Thread.currentThread
  //getName获得当前线程的名称
  println(s"I am the thread $t.name")

  println(s"I am the thread $name")
}


object ThreadsStart extends App {
  class MyThread extends Thread {
    /**
     * 启动线程
     */
    override def run(): Unit = {
      //最后打印
      println(s"I am ${Thread.currentThread.getName}")
    }
  }

  val t = new MyThread()
  t.start()
  //先打印
  println(s"I am ${Thread.currentThread.getName}")
}


object ThreadsCreation extends App {

 /**
  *线程创建
  */
  class MyThread extends Thread {
    override def run(): Unit = {
      //先打印
      println("New thread running.")
    }
  }
  val t = new MyThread

  t.start()
  /**
   * 将main线程切换到等待状态,直到变量t中的新线程执行完毕为止,重点是处于等待状态的线程会交出处理器控制权,
   * OS可以将处理器分配给其他等待可运行线程.
   */
  t.join()
  //最后打印
  println("New thread joined.")
}


object ThreadsSleep extends App{
  val t = thread {
    Thread.sleep(1000)
    log("New thread running.")
    Thread.sleep(1000)
    log("Still running.")
    Thread.sleep(1000)
    log("Completed.")
  }
   /**
   * 将main线程切换到等待状态,直到变量t中的新线程执行完毕为止,重点是处于等待状态的线程会交出处理器控制权,
   * OS可以将处理器分配给其他等待可运行线程.
   */
  t.join()
  //最后打印
  log("New thread joined.")

}

//线程不确定性
object ThreadsNondeterminism extends App {
  /**
   * log语句,在变量t线程中的调用log方法的语句之前,可能之后出现.大多数线程非确定
   * main: ...
   * Thread-0: New thread running.
   * main: ...
   * main: New thread joined.
   */
  val t = thread { log("New thread running.") }
  log("...")
  log("...")
  t.join()
  log("New thread joined.")

}

//线程通信
object ThreadsCommunicate extends App {
   /**
    * t.join()彼此等待对方直到执行完毕,交出处理器控制权之前执行,对result变量执行的赋值操作.
    * 所以result永远不会为null值
    */
  var result: String = null
  val t = thread { result = "Title" + "=" * 5 }
  t.join()
  log(result)
}

/***
 * 线程不安全访问
 */
object ThreadsUnprotectedUid extends App {
  var uidCount = 0L//并发方式读取变量uidCount的值,该变量的初始值为0,
  def getUniqueId() = {
    val freshUid = uidCount + 1  //freshUid 是一个局部变量,它获得是线程栈内存,所有线程都能够看到该变量的独立实例
    uidCount = freshUid 
    freshUid
  }

  def printUniqueIds(n: Int): Unit = {
    //yield产生数组
    val uids = for (i <- 0 until n) yield getUniqueId()
    log(s"Generated uids: $uids")
  }
  //二个线程处理
  val t = thread {
    printUniqueIds(5)//线程并发执行
  }
  //main线程执行
  printUniqueIds(5)
  t.join()
  /**
   * 这两个线程通过随机顺序将数值1写回变量uidCount中,然后它们都返回一个非唯一标识符串1
   * Thread-0: Generated uids: Vector(1, 11, 13, 15, 17)
	 * main: Generated uids: Vector(1, 3, 5, 7, 9)
	 * 
   */
  
}
/**
 * 不使用synchronized语句导致的严重错误,使用布尔值绕过synchronized语句
 */

object ThreadSharedStateAccessReordering extends App {
   /**
    * 下面的程序中两个线程(T1和T2)访问一对布尔变量(a和b)和一对整型变量(x和y),
    * 线程t1将变量a设置为true,然后读取变量b的值,如果变量b的值为true,那么线程t1就会将0赋予变量y
    * 否则将1赋予变量y
    */
  for (i <- 0 until 100000) {
    var a = false
    var b = false
    var x = -1
    var y = -1

    val t1 = thread {
      //Thread.sleep(2)
      a = true 
      y = if (b) 0 else 1
    }
    val t2 = thread {
      //Thread.sleep(2)
      b = true
      x = if (a) 0 else 1
    }
    /**
     * 以上程序范的错误一个线程的写入操作能够立刻被其他线程读到,要确保其他线程能够读一个线程写入操作的情况
     * 就必须适当使用同步化机制
     */
  
    t1.join()
    t2.join()
    assert(!(x == 1 && y == 1), s"x = $x, y = $y")
  }
}
