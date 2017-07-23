package ch3


import org.learningconcurrency._
import ch3._

import java.io._
import java.util.concurrent._
import java.util.concurrent.atomic._
import scala.annotation.tailrec
import scala.collection._
import scala.collection.convert.decorateAsScala._
import org.apache.commons.io.FileUtils

/***
 * 文件系统API必须能够做到以下列几点:
 * 1)当某个线程创建新文件时,将该文件设置无法复制和删除
 * 2)当一个或多个线程正在复制文件时,将该文件设置为无法删除
 * 3)当某个线程删除某个文件时,将该文件设置为无法复制
 * 4)在文件管理中同一时刻只能由单个线程删除文件
 */

object FileSystemTest extends App {
  val fileSystem = new FileSystem(".")

  fileSystem.logMessage("Testing log!")

  fileSystem.deleteFile("test.txt")

  fileSystem.copyFile("build.sbt", "build.sbt.backup")

  val rootFiles = fileSystem.filesInDir("")
  log("All files in the root dir: " + rootFiles.mkString(", "))
}


class FileSystem(val root: String) {
//一个名logger的独立守护线程
  val logger = new Thread {
    setDaemon(true)
    override def run() {
      while (true) {
        /**
         * 调用take方法是出队操作的阻塞版本,它会阻止线程logger调用它,直到队列中含有消息为止.
         * 调用take后,线程logger会通过调用log方法输出消息.
         */
        val msg = messages.take() 
        log(msg)
      }
    }
  }
//一旦main线程终止运行后,logger线程也会自动停止运行
  logger.start()
/**
 * LinkedBlockingQueue 一个由链接节点支持的可选有界队列
 * 该队列声明为messages的私有变量,
 */
  private val messages = new LinkedBlockingQueue[String]
/**
 * messages方法仅会调用队列messages中的offer,也可以使用add或者put方法,因为这个队列是无界的
 * 所以这些方法永远都不会抛出异常或者阻塞调用它们的线程
 */
  def logMessage(msg: String): Unit = messages.add(msg)
//表示特征的状态
  sealed trait State
//空闲
  class Idle extends State
//正在创建 
  class Creating extends State
//正在复制状态中,字段n还会追踪当前以并发方式正在执行的复制操作的数量
  class Copying(val n: Int) extends State
//正在删除
  class Deleting extends State
//isDir字段表明了相应的对象是文件还是目录,
  class Entry(val isDir: Boolean) {
    //AtomicReference则对应普通的对象引用,也就是它可以保证你在修改对象引用时的线程安全性。
    val state = new AtomicReference[State](new Idle)
  }
//该并发映射含有路径和相应的Entry对象,创建了FileSystem对象后,并发Map就会被添加数据
  val files: concurrent.Map[String, Entry] =
    //new ConcurrentHashMap().asScala
   new concurrent.TrieMap()//TrieMap确保执行删除或复制文件操作的线程,无法干扰执行读取文件操作的线程
 //iterateFiles 第二个参数过滤器，第三个参数是否递归
  for (file <- FileUtils.iterateFiles(new File(root), null, false).asScala) {
    //asScala方法能够确保java集合可以获得scala集合API.
    files.put(file.getName, new Entry(false))//文件名和实体对象(false)文件
  }
/**
 * prepareForDelete方法会先读取原子变量的state的引用,然后将该值存储到局部变量S0中.
 * 之后该方法会检查局部变量s0的值是否为Idle,并尝试以原子处理方式将该值更改为Deleting
 * 当另一个线程正在创建或复制该文件时,就无法复制这个文件,因此该文件管理器报告错误并返回false
 * 如果另一个线程已经删除了这个文件,那么该文件管理器仅会返回false
 * 原子变量Stats作用就像锁一样,尽管它即没有阻塞 其他线程也没有使用其他线程处于等待状态,如果prepareForDelete返回true
 * 就说明我们可以用线程安全删除这个文件,因为我们使用线程是唯一一个将state变量的值更改为delete的线程
 * 如果prepareForDelete方法返回false,文件管理器就会在UI中报告错误,而不会阻塞执行删除操作的线程
 * 
 */
  @tailrec private def prepareForDelete(entry: Entry): Boolean = {
    val s0 = entry.state.get
    s0 match {
      case i: Idle =>
        /**
         * compareAndSet 如果当前值 == 预期值，则以原子方式将该值设置为给定的更新值。
         * 参数：
         * expect - 预期值
         * update - 新值
         * 返回：如果成功，则返回 true。返回 false 指示实际值与预期值不相等。
         */        
        if (entry.state.compareAndSet(s0, new Deleting)) true
        else prepareForDelete(entry)
      case c: Creating =>
        logMessage("File currently being created, cannot delete.")
        false
      case c: Copying =>
        logMessage("File currently being copied, cannot delete.")
        false
      case d: Deleting =>
        false
    }
  }
  /**
   * prepareForDelete方法通过原子方式锁定文件,以便执行删除操作,然后从files变量存储的并发映射中删除该文件,
   * 可通过实现deleteFile方法删除目录
   */

  def deleteFile(filename: String): Unit = {
    files.get(filename) match {
      case None =>
        logMessage(s"Cannot delete - path '$filename' does not exist!")
      case Some(entry) if entry.isDir =>
        logMessage(s"Cannot delete - path '$filename' is a directory!")
      case Some(entry) =>
        /**
         * execute方法异步方式删除该文件,该方法不阻塞调用者线程,通过调用execute方法运行的并发任务,会调用prepareForDelete方法
         * 如果返回true,那么调用deleteQuietly方法的操作就是安全.
         */
        execute {
          if (prepareForDelete(entry)) {
            //安全删除
            if (FileUtils.deleteQuietly(new File(filename)))
              files.remove(filename)//该文件就从变量files存储的并发Map中删除.
          }
        }
    }
  }
/**
 * 只有当文件处于Idle或Copying状态时,才能对其执行复制操作,因此我们需要通过原子方式将文件状态从Idle切换到Copying
 * 或者通过增量值n将文件状态从一个Copying状态切换到另一个Copying状态
 */
  @tailrec private def acquire(entry: Entry): Boolean = {//获取acquire
    val s0 = entry.state.get
    s0 match {
      case _: Creating | _: Deleting =>
        logMessage("File inaccessible, cannot copy.")
        false
      case i: Idle =>
        if (entry.state.compareAndSet(s0, new Copying(1))) true
        else acquire(entry)
      case c: Copying =>
        /**
         * compareAndSet方法作用是首先检查当前引用是否等于预期引用，并且当前标志是否等于预期标志，如果全部相等，
         * 则以原子方式将该引用和该标志的值设置为给定的更新值
         */
        if (entry.state.compareAndSet(s0, new Copying(c.n + 1))) true
        else acquire(entry)
    }
  }
/**
 * 线程完成复制文件操作后,该线程必须释放Copying锁,通过相应的release方法可以做到
 * 该方法减少Copying计数或者将文件状态更改为Idle,此处要点是:必须在被复制的文件创建完成.
 * 刚从Creating状态切换到Idle状态后就调用该方法
 */
  @tailrec private def release(entry: Entry): Unit = {
    val s0 = entry.state.get
    s0 match {
      case i: Idle =>
        sys.error("Error - released more times than acquired.")
      case c: Creating =>
        if (!entry.state.compareAndSet(s0, new Idle)) release(entry)
      case c: Copying if c.n <= 0 =>
        sys.error("Error - cannot have 0 or less copies in progress!")
      case c: Copying =>
        val newState = if (c.n == 1) new Idle else new Copying(c.n - 1)
        if (!entry.state.compareAndSet(s0, newState)) release(entry)
      case d: Deleting =>
        sys.error("Error - releasing a file that is being deleted!")
    }
  }
/***
 * 该方法会检查files映射中包含该条目,那么copyFile方法就会通过运行一个并发复制该条目代表的文件
 * 
 */
  def copyFile(src: String, dest: String): Unit = {
    files.get(src) match {
      case None =>
        logMessage(s"File '$src' does not exist.")
      case Some(srcEntry) if srcEntry.isDir =>
        sys.error(s"Path '$src' is a directory!")
      case Some(srcEntry) =>
        execute {
          if (acquire(srcEntry)) try {
            //acquire获取该文件的监控器以便执行复制操作,并创建一个代表creating状态的destEntry条目
            val destEntry = new Entry(false)            
            destEntry.state.set(new Creating)
            //putIfAbsent 返回与指定键关联的以前的值，或如果没有键映射None
            /**
             * putIfAbsent该方法会检查files映射中是否含有代表文件路径dest的键
             * 如果files映射没有dest和destEntry键值对,该方法向files添加该条目            
             * 此刻srcEntry和destEntry条目都会锁定,
             */
            if (files.putIfAbsent(dest, destEntry) == None) try {
              //文件复制
              FileUtils.copyFile(new File(src), new File(dest))
            } finally release(destEntry)//释放destEntry
          } finally release(srcEntry)//释放srcEntry
        }
    }
  }

  def filesInDir(dir: String): Iterable[String] = {
    // trie map snapshots
    for ((name, state) <- files; if name.startsWith(dir)) yield name
  }

}
