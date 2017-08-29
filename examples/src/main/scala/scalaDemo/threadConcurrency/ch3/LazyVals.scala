
package ch3

import org.learningconcurrency._
import ch3._



/**
 * 惰性值:是指在第一次被读取时,由其值定义中右侧表达式对其进行初始化的值,
 * 在与普通值不同,普通值会在被创建时被初始化,如果惰性值在程序中从没有被读取,那么永远不会初始化
 * 因此也不会付出初始化所需的代价.
 */

object LazyValsCreate extends App {
  import scala.concurrent._
  /**
   * 惰性值只应在线程访问它时进行初始化,而且其初始化操作至多只能执行一次,
   * 两个线程访问了两个obj和non值
   */
  lazy val obj = new AnyRef
  lazy val nondeterministic = s"made by ${Thread.currentThread.getName}"

  execute {
    log(s"Execution context thread sees object = $obj")
    log(s"Execution context thread sees nondeterministic = $nondeterministic")
  }

  log(s"Main thread sees object = $obj")
  log(s"Main thread sees nondeterministic = $nondeterministic")
}

/**
 * 惰性值
 */
object LazyValsObject extends App {
  object Lazy {
    log("Running Lazy constructor.")
  }

  log("Main thread is about to reference Lazy.")
  Lazy //在第四行代码中第一次被引用,其初始化方法才会运行,而且不是在声明该对象是运行初始化方法
  log("Main thread completed.")
}


object LazyValsUnderTheHood extends App {
  @volatile private var _bitmap = false
  private var _obj: AnyRef = _
  def obj = if (_bitmap) _obj else this.synchronized {
    if (!_bitmap) {
      _obj = new AnyRef
      _bitmap = true
    }
    _obj
  }

  log(s"$obj"); log(s"$obj")
}


object LazyValsInspectMainThread extends App {
  //Thread.currentThread().getContextClassLoader,可以获取当前线程的引用,getContextClassLoader用来获取线程的上下文类加载器
  val mainThread = Thread.currentThread

  lazy val x = {
    log(s"running Lazy ctor")
    Thread.sleep(1000)
    log(s"main thread state - ${mainThread.getState}")
  }

  execute { x }

  log("started asynchronous thread")
  Thread.sleep(200)
  log("log about to access x")
  x
}


object LazyValsDeadlock extends App {
  object A {
    lazy val x: Int = B.y
  }
  object B {
    lazy val y: Int = A.x
  }

  execute { B.y }

  A.x
}


object LazyValsAndSynchronized extends App {
  lazy val terminatedThread = {
    val t = ch2.thread {
      LazyValsAndSynchronized.synchronized {}
    }
    t.join()
    t.getState
  }

  terminatedThread
}


object LazyValsAndBlocking extends App {
  lazy val x: Int = {
    val t = ch2.thread {
      println(s"Initializing $x.")
    }
    t.join()
    1
  }
  x
}


object LazyValsAndMonitors extends App {
  lazy val x = 1
  this.synchronized {
    val t = ch2.thread { x }
    t.join()
  }
}




