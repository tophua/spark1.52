package ch4
import org.learningconcurrency._
import scala.concurrent.Future

/**
 * 如果说futures是为了一个还没有存在的结果，而当成一种只读占位符的对象类型去创建，
 * 那么promise就被认为是一个可写的，可以实现一个future的单一赋值容器,
 * promise通过这种success方法可以成功去实现一个带有值的future. 
 */
object PromisesCreate extends App {
  /**
   * 创建Promisesc对象
   */
  import scala.concurrent._
  import ExecutionContext.Implicits.global

  val p = Promise[String]//存储字符串
  val q = Promise[String]
  //Promise对象p关联future对象中,添加了回调函数foreach
  p.future.foreach {
    case text => log(s"Promise p succeeded with '$text'")
  }
  //等待1秒时间,在通过调用success方法完善对象p之前,该回调函数不会被调用
  Thread.sleep(1000)
  p.success("kept")
  //trySuccess分别对应success方法,该方法仅会返回表明该赋值操作是否成功的布尔值
  val secondAttempt = p.trySuccess("kept again")

  log(s"Second attempt to complete the same promise went well? $secondAttempt")
  //q关联的future对象执行执行完善操作失败的情况,并在该future对象中添加了回调函数failed foreach 
  q.failure(new Exception("not kept"))
  q.future.failed.foreach {
    case t => log(s"Promise q failed with $t")
  }
  Thread.sleep(1000)
}

/**
 * 自定义方法异步计算
 */
object PromisesCustomAsync extends App {
  import scala.concurrent._
  import ExecutionContext.Implicits.global
  import scala.util.control.NonFatal
/**
 * myFuture方法接收一个名为body的命名参数,该参数是通过异步计算获得
 * 首先程序会创建一个Promise对象,然后,它会在global执行上下文中执行一个异步计算操作
 * 该计算操作会获得body的值并完善Promise对象,如果body的代码主体抛出了一个非致命异常,
 * 该异步计算操作会因这个异常而无法完善Promise对象,
 * 同时myFuture方法会在该异步计算操作开始执行后, 立即返回Future对象.
 * 
 * 这是常见生成Future对象的模式,先创建Promise对象,然后通过其他计算操作完善这个Promise对象
 * 并返回相应的Future对象,
 */
  def myFuture[T](body: =>T): Future[T] = {
    val p = Promise[T]
    global.execute(new Runnable {
      def run() = try {
        val result = body
        p.success(result)
      } catch {
        case NonFatal(e) =>
          p.failure(e)
      }
    })

    p.future
  }

  val future = myFuture {
    "naaa" + "na" * 8 + " Katamari Damacy!"
  }

  future.foreach {
    case text => log(text)
  }
  Thread.sleep(500)

}

/**
 * 转换基于回调函数API
 */
object PromisesAndCallbacks extends App {
  import scala.concurrent._
  import ExecutionContext.Implicits.global
  import org.apache.commons.io.monitor._
  import java.io.File

  def fileCreated(directory: String): Future[String] = {
    val p = Promise[String]//创建一个Promise对象,然后通过一些计算操作延迟完善该Promise对象的操作
    //FileAlterationMonitor充许订阅文件系统事件,如创建和删除文件与目录
    //FileAlterationMonitor对象会定期扫描文件系统,以查明其中的改变,之后我们需要再创建一个FileAlterationObserver对象
    //该对象代表回调函数,当某个文件在文件系统中被创建时,FileAlterationObserver对象中onFileCreate会调用
    
    val fileMonitor = new FileAlterationMonitor(1000)
    val observer = new FileAlterationObserver(directory)
    val listener = new FileAlterationListenerAdaptor {
      //onFileCreate方法会接收目录的名称并返回一个Future对象,该对象中含有新建目录中第一个文件的名称
      override def onFileCreate(file: File) {
        try p.trySuccess(file.getName)//trySuccess分别对应success方法,该方法仅会返回表明该赋值操作是否成功的布尔值
        finally fileMonitor.stop()
      }
    }
    observer.addListener(listener)
    fileMonitor.addObserver(observer)
    fileMonitor.start()

    p.future
  }
/**
 * 使用Future对象,订阅文件系统中第一个被改变的文件,fileCreated方法返回的Future对象中的foreach方法
 * 在编辑器中创建一个新文件,并观察该程序检测新建文件的方式.
 */
  fileCreated(".") foreach {
    case filename => log(s"Detected new file '$filename'")
  }
Thread.sleep(500)
}


object PromisesAndCustomOperations extends App {
  import scala.concurrent._
  import ExecutionContext.Implicits.global

  implicit class FutureOps[T](val self: Future[T]) {
    def or(that: Future[T]): Future[T] = {
      val p = Promise[T]
      self onComplete { case x => p tryComplete x }
      that onComplete { case y => p tryComplete y }
      p.future
    }
  }

  val f = Future { "now" } or Future { "later" }

  f foreach {
    case when => log(s"The future is $when")
  }

}


object PromisesAndTimers extends App {
  import java.util._
  import scala.concurrent._
  import ExecutionContext.Implicits.global
  import PromisesAndCustomOperations._

  private val timer = new Timer(true)
/**
 * 判断超时是一种无法通过Future对象获得的实用功能,
 * 该方法接收代表毫秒数值的变量t,并在变量t限定的时间内返回已完善的Future对象,
 * 
 */
  def timeout(millis: Long): Future[Unit] = {
    val p = Promise[Unit]//首先创建Promise对象,再没有完善之前,该对象中含有的信息都是没有意义的
    timer.schedule(new TimerTask {
     //通过TimerTask调用Time类中的schedule方法,TimerTask对象会在变量t限定的时间内完善Promise对象p
      def run() = p success ()
    }, millis)
    p.future
  }
//由timeout方法返回的Future对象可用于添加回调函数或者通过组合子与其他Future对象组合
  val f = timeout(1000).map(_ => "timeout!") or Future {
    Thread.sleep(999)
    "work completed!"
  }

  f foreach {
    case text => log(text)
  }

}


object PromisesCancellation extends App {
  import scala.concurrent._
  import ExecutionContext.Implicits.global

  def cancellable[T](b: Future[Unit] => T): (Promise[Unit], Future[T]) = {
    val p = Promise[Unit]
    val f = Future {
      val r = b(p.future)
      if (!p.tryFailure(new Exception))
        throw new CancellationException
      r
    }
    (p, f)
  }

  val (cancel, value) = cancellable { cancel =>
    var i = 0
    while (i < 5) {
      if (cancel.isCompleted) throw new CancellationException
      Thread.sleep(500)
      log(s"$i: working")
      i += 1
    }
    "resulting value"
  }

  Thread.sleep(1500)

  cancel trySuccess ()

  log("computation cancelled!")
}


