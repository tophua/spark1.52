package sparkDemo

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.SparkEnv.{logDebug, logInfo}
import org.apache.spark.rpc.{RpcEndpoint, RpcEndpointRef}
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.RpcUtils



/**
  * Created by liush on 17-8-5.
  * trait支持部分实现，也就是说可以在scala的trait中可以实现部分方法
  * trait和抽象类的区别在于抽象类是对一个继承链的,类和类之前确实有父子类的继承关系,而trait则如其名字,表示一种特征,可以多继承。
  */
object InstantiateClass extends App with Logging {
  val conf: SparkConf = new SparkConf()
  def instantiateClass[T](className: String): T = {
    logInfo(s"className: ${className}")
    val cls = classForName(className)
    logInfo(s"cls: ${cls}")
    // Look for a constructor taking a SparkConf and a boolean isDriver, then one taking just
    // SparkConf, then one taking no arguments
    //寻找一个构造函数,使用一个SparkConf和一个布尔值为isDriver的代码,然后需要一个参数Boolean的SparkConf构造函数
    //查找一个sparkconf构造函数,是否isDriver
    try {
      //classOf类强制类型转换SparkConf类,classOf[T]`等同于Java中的类文字`T.class`。
      logInfo(s"classOf[SparkConf]: ${classOf[SparkConf]}")

      val tset=cls.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
        .newInstance(conf, new java.lang.Boolean(true))
        //asInstanceOf强制类型[T]对象
        .asInstanceOf[T]
      logInfo(s"asInstanceOf[T]: ${tset.toString}")
      cls.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
        .newInstance(conf, new java.lang.Boolean(true))
        //asInstanceOf强制类型[T]对象
        .asInstanceOf[T]
    } catch {
      case _: NoSuchMethodException =>
        try {
          logInfo(s"asInstanceOf[T]: ${cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]}")
          cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]

        } catch {
          case _: NoSuchMethodException =>
            logInfo(s"1111111 asInstanceOf[T]: ${cls.getConstructor().newInstance()}")
            logInfo(s"asInstanceOf[T]: ${cls.getConstructor().newInstance().asInstanceOf[T]}")

            cls.getConstructor().newInstance().asInstanceOf[T]
        }
    }
  }

  def classForName(className: String): Class[_] = {

    val classLoader = getContextOrSparkClassLoader
    logInfo(s"classLoader: ${classLoader}")
    Class.forName(className, true, getContextOrSparkClassLoader)
    // scalastyle:on classforname
  }
  def getContextOrSparkClassLoader: ClassLoader = {
    //Thread.currentThread().getContextClassLoader,可以获取当前线程的引用,getContextClassLoader用来获取线程的上下文类加载器
    val ContextClassLoader=Thread.currentThread().getContextClassLoader

    logInfo(s"ContextClassLoader: ${ContextClassLoader}")
    //Thread.currentThread().getContextClassLoader,可以获取当前线程的引用,getContextClassLoader用来获取线程的上下文类加载器
  Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)
}
  def getSparkClassLoader: ClassLoader ={
    logInfo(s"getClass.getClassLoader: ${ getClass.getClassLoader}")
    getClass.getClassLoader
  }

  def instantiateClassFromConf[T](propertyName: String, defaultClassName: String): T = {
    instantiateClass[T](conf.get(propertyName, defaultClassName))
  }
  val serializer = instantiateClassFromConf[Serializer](
    "spark.serializer", "org.apache.spark.serializer.JavaSerializer")
  logInfo(s"Using serializer: ${serializer.getClass}")
  println("====="+serializer.getClass)


}
