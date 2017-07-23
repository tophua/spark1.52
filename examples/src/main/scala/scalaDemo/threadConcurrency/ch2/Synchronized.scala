
package ch2


import org.learningconcurrency._
import ch2._

/**
 * synchronized可以确保线程写入操作的可见性,还可以限制对共享内存区域进行的并发访问,限制访问共享资源的同步机制通常称为锁
 */

object SynchronizedProtectedUid extends App {

  var uidCount = 0L
/**
 * synchronized(同步化)确保了由一个线程执行的同步化代码块不会同时再由其他线程执行,
 * 还确保了同一个对象(this对象)中的其他同步代码不会被调用.
 */
  def getUniqueId() = this.synchronized {
    val freshUid = uidCount + 1
    uidCount = freshUid
    freshUid
  }

  def printUniqueIds(n: Int): Unit = {
    val uids = for (i <- 0 until n) yield getUniqueId()
    println(s"Generated uids: $uids")
  }

  val t = thread{
    printUniqueIds(5)
  }
  printUniqueIds(5)
  t.join()

}
/**
 * this.synchronized写入操作都是原子化,而且对象x执行synchronized语句的所有线程都能够读到其他线程执行这些写入操作的情况
 */

// we should skip this one
//调整次序
object SynchronizedSharedStateAccess extends App {
  for (i <- 0 until 10000) {
    var t1started = false
    var t2started = false
    var t1index = 0
    var t2index = 0
  //下面两个线程(t1,t2)访问一对布尔型变量(a和b)和一对变量(x,y)
  //线程t1将变量a设置为true,然后记取变量b的值,如果变量b的值为true
  //线程t1就会将0赋予变量y,否则就会将1赋予变量y
    val t1 = thread {
      Thread.sleep(1)    
      //当出现多个线程都会访问读取或写入某个状态时,就应该对对象使用synchronized语句
      //这样做可以在最大程度上确保单个线程随时执行对象中synchronized语句,可以确保一个线程对内存
      //执行的所有写入操作,对于同一个对象执行synchronized语句的所有后续线程都是可见的
      this.synchronized { t1started = true }
      val t2s = this.synchronized { t2started }
      t2index = if (t2started) 0 else 1
    }
    val t2 = thread {
      Thread.sleep(1)
     /**
     * 一个线程的写入操作能够立刻被其他线程读到,要确保其他线程能够读一个线程写入操作的情况,就必须适当使用同步化机制   
     */
      this.synchronized { t2started = true }
      val t1s = this.synchronized { t1started }
      t1index = if (t1s) 0 else 1
    }
  
    t1.join()
    t2.join()
    assert(!(t1index == 1 && t2index == 1), s"t1 = $t1index, t2 = $t2index")
  }
}

/**
 * Synchronized嵌套,一个线程可以同时拥有多个对象的监控器
 */
object SynchronizedNesting extends App {
  import scala.collection._
  private val transfers = mutable.ArrayBuffer[String]()
  /**
   * ArrayBuffer数组实现的是一个专门由单线程使用的集合,因此我们应该防止它被执行并发写入操作
   */
  def logTransfer(name: String, n: Int): Unit = transfers.synchronized {
    transfers += s"transfer to account '$name' = $n"
  }
  /**
   * 账号Account对象含有其所有者的信息以及他们拥有的资金数额
   */
  class Account(val name: String, var money: Int)
  /**
   * 向账号中充值,该系统会使用add方法获取指定Account对象的监控器
   * 并修改该对象中的money字段
   */
  def add(account: Account, n: Int) = account.synchronized {//获得transfers监控器
    account.money += n//如果转账金额超过10个货币单位,需要该操作记录下来
    if (n > 10) logTransfer(account.name, n)
  }
  //主应用程序创建两个独立的账号和用于执行转账的3个线程,一旦所有线程都完成了它们的转账操作
  //main线程就会输出所有已记录的转账信息
  val jane = new Account("Jane", 100)
  val john = new Account("John", 200)
  val t1 = thread { add(jane, 5) }
  val t2 = thread { add(john, 50) }
  val t3 = thread { add(jane, 70) }
  /**
   * 这个例子使用synchronized语句,能够防止线程t1和t3以并发方式修改Janer 的账号信息,破坏这个账号
   * 线程t2和t3还会访问transfers监控器的使用记录
   */
  t1.join(); 
  t2.join(); 
  t3.join()
  log(s"--- transfers ---\n$transfers")
}

/**
 * 死锁:是指两个或多个控制流实体在继续执行自己的操作前,等待对方先完成操作的情况.
 * 等待的原因是每个控制流实体都独占了,其他控制流实体必须获得后才能继续执行其他操作的资源
 * 
 */
object SynchronizedDeadlock extends App {
  import SynchronizedNesting.Account
  //
  def send(a: Account, b: Account, n: Int) = a.synchronized {
    b.synchronized {
      a.money -= n
      b.money += n
    }
  }
  val a = new Account("Jill", 1000)
  val b = new Account("Jack", 2000)
  val t1 = thread { for (i <- 0 until 100) send(a, b, 1) }
  val t2 = thread { for (i <- 0 until 100) send(b, a, 1) }
  t1.join()
  t2.join()
  log(s"a = ${a.money}, b = ${b.money}")
}

/**
 * 防止死锁的方法
 */
object SynchronizedNoDeadlock extends App {
  import SynchronizedProtectedUid._
  class Account(val name: String, var money: Int) {
    val uid = getUniqueId()//获取账号的顺序
  }
  def send(a1: Account, a2: Account, n: Int) {
    def adjust() {
      a1.money -= n
      a2.money += n
    }
    if (a1.uid < a2.uid)
      //
      a1.synchronized { a2.synchronized { adjust() } }
    else
      a2.synchronized { a1.synchronized { adjust() } }
  }
  val a = new Account("Jill", 1000)
  val b = new Account("Jack", 2000)
  val t1 = thread { for (i <- 0 until 100) send(a, b, 1) }
  val t2 = thread { for (i <- 0 until 100) send(b, a, 1) }
  t1.join()
  t2.join()
  log(s"a = ${a.money}, b = ${b.money}")
}

//同步重复
object SynchronizedDuplicates extends App {
  import scala.collection._
  val duplicates = mutable.Set[Int]()
  val numbers = mutable.ArrayBuffer[Int]()
  def isDuplicate(n: Int): Unit = duplicates.synchronized {
    duplicates.contains(n)
  }
  def addDuplicate(n: Int): Unit = duplicates.synchronized {
    duplicates += n
  }
  def addNumber(n: Int): Unit = numbers.synchronized {
    numbers += n
    if (numbers.filter(_ == n).size > 1) addDuplicate(n)
  }
  val threads = for (i <- 1 to 2) yield thread {
    for (n <- 0 until i * 10) addNumber(n)
  }
  for (t <- threads) t.join()
  println(duplicates.mkString("\n"))
}

/**
 * 线程池:同一个线程应该由许多个请求反复使用,这些可重复的线程组通常称为线程池
 */
object SynchronizedBadPool extends App {
  import scala.collection._
  /**
 * 使用Queue存储被调度的代码块,使用函数() => Unit设置这些代码块
 */
  private val tasks = mutable.Queue[() => Unit]()
  /**
 * 线程Worker反复调用poll方法,在变量task存储的对象中实现同步化,
 * 检查该对象代表的队列是否为空,poll方法展示了synchronized语句可以返回一个值
 */
  val worker = new Thread {
    def poll(): Option[() => Unit] = tasks.synchronized {
      //返回一个可选的Some值,否则该语句返回一个None,dequeue弹出队列
      if (tasks.nonEmpty) Some(tasks.dequeue()) else None
    }
   
    override def run() = while (true) poll() match {
      case Some(task) => task()//如果方法则调用
      case None =>
    }
  }
  //设置守护线程,设置守护线程的原因,synchronized方法向它发送任务,
  //该方法会调用指定的代码块,以便最终执行worker线程
  worker.setDaemon(true)
  //运行work线程
  worker.start()
  //设置守护线程的原因,synchronized方法向它发送任务,
  //该方法会调用指定的代码块,以便最终执行worker线程
  def asynchronous(body: =>Unit) = tasks.synchronized {
    tasks.enqueue(() => body)//插入队列
  }

  asynchronous { log("Hello") }
  asynchronous { log(" world!")}
  Thread.sleep(100)
}

/**
 * 同步守卫的块,main线程准备了Some消息,使之显示该消息
 */
object SynchronizedGuardedBlocks extends App {
  val lock = new AnyRef //locak对象中的监控器,
  var message: Option[String] = None
  //greeter线程通过获取lock对象的监控器开始其运行过程,并检查main线程为其准备的消息是否为None类型
  //如果该消息为None类型,那么greeter线程就不会显示任务信息,而且会调用lock对象中的wait方法
  val greeter = thread {
    lock.synchronized {
      //当线程T调用了某个对象中的wait方法后,线程T就会释放该对象的监控器并切换到等待状态,直到其他线程调用了
      //该对象中的notify方法后,线程T才会切换回正在运行状态.
      while (message == None) lock.wait()//将线程切换为等待状态,并释放资源和锁
      log(message.get)
    }
  }
  lock.synchronized {
    message = Some("Hello!")
    lock.notify()//将线程切换为正在运行状态,并释放资源和锁
  }
  greeter.join()
}

/**
 * 线程池:同一个线程应该由许多个请求反复使用,这些可重复的线程组通常称为线程池
 */
object SynchronizedPool extends App {
  import scala.collection._
/**
 * 使用Queue存储被调度的代码块,使用函数() => Unit设置这些代码块
 */
  private val tasks = mutable.Queue[() => Unit]()
/**
 * 线程Worker反复调用poll方法,在变量task存储的对象中实现同步化,
 * 检查该对象代表的队列是否为空,poll方法展示了synchronized语句可以返回一个值
 */
  object Worker extends Thread {//将Worker设置一个单例对象,在该程序中,由poll线程调用tasks对象中的wait方法
    //然后该线程进入等待状态,直到main线程向tasks对象中添加一个代码块并调用synchronized方法中的notify方法为止
    
    setDaemon(true)
    def poll() = tasks.synchronized {
      //空等待,耗费资源
      while (tasks.isEmpty) tasks.wait()
      tasks.dequeue()
    }
    override def run() = while (true) {
      val task = poll() // poll() 方法都是从队列中删除第一个元素（head）,在用空集合调用时不是抛出异常
      task()
    }
  }

  Worker.start()

  def asynchronous(body: =>Unit) = tasks.synchronized {
    tasks.enqueue(() => body)
    tasks.notify()
  }

  asynchronous { log("Hello ") }
  asynchronous { log("World!") }
}

//同步优雅关闭线程
object SynchronizedGracefulShutdown extends App {
  import scala.collection._
  import scala.annotation.tailrec
  //Queue 一个队列就是一个先入先出（FIFO）的数据结构,
  //
  private val tasks = mutable.Queue[() => Unit]()

  object Worker extends Thread {
    var terminated = false
    def poll(): Option[() => Unit] = tasks.synchronized {
      //wait将线程切换为等待状态,并释放资源和锁
      while (tasks.isEmpty && !terminated) tasks.wait()
      if (!terminated) Some(tasks.dequeue()) else None
    }
    //注意@tailrec,run尾递归调用
    @tailrec override def run() = poll() match {
      case Some(task) => task(); run()
      case None =>
    }
    def shutdown() = tasks.synchronized {
      terminated = true
      //唤醒线程,将线程切换为正在运行状态,,并释放资源和锁
      tasks.notify()
    }
  }

  Worker.start()

  def asynchronous(body: =>Unit) = tasks.synchronized {
    tasks.enqueue(() => body)
    //唤醒线程,将线程切换为正在运行状态,并释放资源和锁
    tasks.notify()
  }

  asynchronous { log("Hello ") }
  asynchronous { log("World!") }

  Thread.sleep(1000)
  //线程正常关闭,
  Worker.shutdown()
}



