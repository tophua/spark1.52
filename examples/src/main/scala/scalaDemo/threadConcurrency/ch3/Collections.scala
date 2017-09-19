package ch3

import org.learningconcurrency._
import ch3._
/**
 * 预测多个线程对集合状态产生影响,使用两个线程向ArrayBuffer集合中添加数字
 */
object CollectionsBad extends App {
  import scala.collection._
/**
 * 标准集合的实现代码中没有使用任何同步机制,可变集合的基础数据结构可能会非常复杂.
 */
  val buffer = mutable.ArrayBuffer[Int]()

  def add(numbers: Seq[Int]) = execute {
    buffer ++= numbers
    log(s"buffer = $buffer")
  }
  /**
   * 这个例子不会输出含有20个不同数值元素的ArrayBuffer对象,而会在每次运行时输出不同的随机结果或者抛出异常
    * ForkJoinPool-1-worker-5: buffer = ArrayBuffer(10, 11, 12, 13, 4, 5, 16, 7, 8, 9)
      ForkJoinPool-1-worker-3: buffer = ArrayBuffer(10, 11, 12, 13, 4, 5, 16, 7, 8, 9)

      ForkJoinPool-1-worker-3: buffer = ArrayBuffer(10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
      ForkJoinPool-1-worker-5: buffer = ArrayBuffer(10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
   */
  add(0 until 10)
  add(10 until 20)
  Thread.sleep(500)
}
/**
 * 同步可变的集合
 */

object CollectionsSynchronized extends App {
  import scala.collection._

  val buffer = new mutable.BufferProxy[Int] with mutable.SynchronizedBuffer[Int] {
    val self = mutable.ArrayBuffer[Int]()
  }

  execute {
    buffer ++= (0 until 10)
    log(s"buffer = $buffer")
  }

  execute {
    buffer ++= (10 until 20)
    log(s"buffer = $buffer")
  }
  /**
  ForkJoinPool-1-worker-5: buffer = ArrayBuffer(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19)
  ForkJoinPool-1-worker-3: buffer = ArrayBuffer(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19)
    */
 Thread.sleep(500)
}


object MiscSyncVars extends App {
  import scala.concurrent._
  val sv = new SyncVar[String]

  execute {
    Thread.sleep(500)
    log("sending a message")
    sv.put("This is secret.")
  }

  log(s"get  = ${sv.get}")
  log(s"take = ${sv.take()}")

  execute {
    Thread.sleep(500)
    log("sending another message")
    sv.put("Secrets should not be logged!")
  }

  log(s"take = ${sv.take()}")
  log(s"take = ${sv.take(timeout = 1000)}")
}


object MiscDynamicVars extends App {
  import scala.util.DynamicVariable

  val dynlog = new DynamicVariable[String => Unit](log)
  def secretLog(msg: String) = println(s"(unknown thread): $msg")

  execute {
    dynlog.value("Starting asynchronous execution.")
    dynlog.withValue(secretLog) {
      dynlog.value("Nobody knows who I am.")
    }
    dynlog.value("Ending asynchronous execution.")
  }

  dynlog.value("is calling the log method!")
}

/**
 * main线程创建了一个含有5500个元素的队列,它执行了一个创建迭代器的并发任务,并逐个显示这些元素.与此同时,main线程还按
 * 照遍历顺序,从该队列中逐个删除所有元素.
 * 顺序队列与并发队列之间的一个主要差异是并发队列拥有弱一致性迭代器
 */
object CollectionsIterators extends App {
  import java.util.concurrent._
/**
 * BlockingQueue接口还为顺序队列中已经存在的方法,额外提供了阻塞线程的版本
 */
  val queue = new LinkedBlockingQueue[String]
  //一个队列就是一个先入先出（FIFO）的数据结构,
  //如果想在一个满的队列中加入一个新项，多出的项就会被拒绝,
  //这时新的 offer 方法就可以起作用了。它不是对调用 add() 方法抛出一个 unchecked 异常，而只是得到由 offer() 返回的 false
  for (i <- 1 to 5500) queue.offer(i.toString)//入列
  execute {
    val it = queue.iterator //通过iterator创建迭代器,一旦被创建完成就会遍历队列中的元素,如果遍历操作结束前对队列执行了入列
    //或出列操作,那么该遍历操作就会完全失效
    while (it.hasNext) log(it.next())
  }
  for (i <- 1 to 5500) log(queue.poll())//出列
}

/**
 * ConcurrentHashMap操作,不堵塞线程
 */
object CollectionsConcurrentMap extends App {
  import java.util.concurrent.ConcurrentHashMap
  import scala.collection._
  import scala.collection.convert.decorateAsScala._
  import scala.annotation.tailrec

  val emails = new ConcurrentHashMap[String, List[String]]().asScala

  execute {
    emails("James Gosling") = List("james@javalove.com")
    log(s"emails = $emails")
  }

  execute {
    emails.putIfAbsent("Alexey Pajitnov", List("alexey@tetris.com"))
    log(s"emails = $emails")
  }

  execute {
    emails.putIfAbsent("Alexey Pajitnov", List("alexey@welltris.com"))
    log(s"emails = $emails")
  }

}


object CollectionsConcurrentMapIncremental extends App {
  import java.util.concurrent.ConcurrentHashMap
  import scala.collection._
  import scala.collection.convert.decorateAsScala._
  import scala.annotation.tailrec

  val emails = new ConcurrentHashMap[String, List[String]]().asScala

  @tailrec def addEmail(name: String, address: String) {
    emails.get(name) match {
      case Some(existing) =>
        //
        if (!emails.replace(name, existing, address :: existing)) addEmail(name, address)
      case None =>
        //putIfAbsent()方法用于在 map 中进行添加,这个方法以要添加到 ConcurrentMap实现中的键的值为参数，就像普通的 put() 方法，
        //但是只有在 map 不包含这个键时，才能将键加入到 map 中。如果 map 已经包含这个键，那么这个键的现有值就会保留。
        if (emails.putIfAbsent(name, address :: Nil) != None) addEmail(name, address)
    }
  }

  execute {
    addEmail("Yukihiro Matsumoto", "ym@ruby.com")
    log(s"emails = $emails")
  }

  execute {
    addEmail("Yukihiro Matsumoto", "ym@ruby.io")
    log(s"emails = $emails")
  }

}

/**
 * 创建一个将姓名与数字对应起来的并发映射,例如:Janice会与0对应
 * 如果迭代器具有一致性,我们就会看到初始时映射中会含有3个名字,而且根据第一个任务添加名字的数量
 * John对应的数字会是0至n之间的值,该结果可能是John1,John2,John3,但输出John 8和John 5之类随机不连续名字
 */
object CollectionsConcurrentMapBulk extends App {
  import scala.collection._
  import scala.collection.convert.decorateAsScala._
  import java.util.concurrent.ConcurrentHashMap

  val names = new ConcurrentHashMap[String, Int]().asScala
  names("Johnny") = 0
  names("Jane") = 0
  names("Jack") = 0

  execute {
    for (n <- 0 until 10) names(s"John $n") = n
  }

  execute {
    for (n <- names) log(s"name: $n")
  }
  /**
   * ForkJoinPool-1-worker-3: name: (Jane,0)
   * ForkJoinPool-1-worker-3: name: (Jack,0)
   * ForkJoinPool-1-worker-3: name: (John 8,8)
   * ForkJoinPool-1-worker-3: name: (John 0,0)
   * ForkJoinPool-1-worker-3: name: (John 5,5)
   * ForkJoinPool-1-worker-3: name: (Johnny,0)
   * ForkJoinPool-1-worker-3: name: (John 6,6)
   * ForkJoinPool-1-worker-3: name: (John 4,4)
   */
Thread.sleep(500)
}
/**
 * TrieMap和ConcurrentHashMap区别,如果应用程序 需要使用一致性迭代器使用TrieMap集合
 * 如果应用程序无须 使用一致性迭代器并且极少执行修改并发映射的操作,就应该使用ConcurrentHashMap集合
 * 因为对它们执行查询操作可以获得较快的速度
 */

object CollectionsTrieMapBulk extends App {
  import scala.collection._
/**
 * TrieMap永远不会出现上面的例子,TrieMap并发集合运行同一个程序,并在输出这些名字前按字母顺序对它们进行排序
 * TrieMap确保执行删除或复制文件操作的线程,无法干扰执行读取文件操作的线程
 */
  val names = new concurrent.TrieMap[String, Int]
  names("Janice") = 0
  names("Jackie") = 0
  names("Jill") = 0

  execute {
    for (n <- 10 until 100) names(s"John $n") = n
  }

  execute {
    log("snapshot time!")
    for (n <- names.map(_._1).toSeq.sorted) log(s"name: $n")
  }
Thread.sleep(500)
}





