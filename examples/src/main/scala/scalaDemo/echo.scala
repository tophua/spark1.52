package scalaDemo

import java.lang.management.ManagementFactory

import org.apache.spark.rdd.RDD
import org.apache.spark.util.SizeEstimator.logWarning
import scala.collection.JavaConverters._
import scala.collection.immutable

/**
  * Created by liush on 17-8-7.
  */
object echo extends  App{
  /**
    * 在参数的类型之后放一个星号,向函数传入可变长度参数列表
    * @param args
    */
  def echo(args: String*) =
    for (arg <- args) println(arg)
  echo()
  echo("one")
  echo("hello", "world!")
  val arr = Array("What's", "up", "doc?")
 // echo(arr)
  //数组参数后添加一个冒号和一个 _* 符号,告诉编译器把 arr 的每个元素当作参数，而不是当作单一的参数传给
  echo(arr: _*)
  //rdd: RDD[_], others: RDD[_]*
  def echoa(others:RDD[_], rdd:RDD[_]) =
    for (arg <- rdd) println(arg)
  try {
    val hotSpotMBeanName = "com.sun.management:type=HotSpotDiagnostic"
    val server = ManagementFactory.getPlatformMBeanServer()
    println("server===="+server)
    // NOTE: This should throw an exception in non-Sun JVMs
    //注意：这应该在非Sun JVM中引发异常
    // scalastyle:off classforname
    //HotSpotDiagnosticMXBean获得运行期间的堆的dump文件
    val hotSpotMBeanClass = Class.forName("com.sun.management.HotSpotDiagnosticMXBean")
    println("hotSpotMBeanClass===="+hotSpotMBeanClass)
    val getVMMethod = hotSpotMBeanClass.getDeclaredMethod("getVMOption",
      Class.forName("java.lang.String"))
    // scalastyle:on classforname
    println("getVMMethod===="+getVMMethod)
    val bean = ManagementFactory.newPlatformMXBeanProxy(server,
      hotSpotMBeanName, hotSpotMBeanClass)
    println("bean===="+bean)
    // TODO: We could use reflection on the VMOption returned ?
    //压缩普通对象指针
     val tset=getVMMethod.invoke(bean, "UseCompressedOops").toString.contains("true")
    println("UseCompressedOops===="+tset)
  } catch {
    case e: Exception => {
      // Guess whether they've enabled UseCompressedOops based on whether maxMemory < 32 GB
      //猜测他们是否根据maxMemory <32 GB启用了UseCompressedOops
      val guess = Runtime.getRuntime.maxMemory < (32L*1024*1024*1024)
      val guessInWords = if (guess) "yes" else "not"
      println("Failed to check whether UseCompressedOops is set; assuming " + guessInWords)

    }
  }

  def env: immutable.Map[String, String] = immutable.Map(System.getenv().asScala.toSeq: _*)
  env.map(println _)

  println(System.getProperty("user.dir"))

  println(sys.props)
  //System.getProperties()获取系统参数
  println( "=========="+System.getProperties())

}
