package sparkDemo

import java.io.File

import scala.util.Properties.javaClassPath

/**
  * Created by liush on 17-8-6.
  */
object ClassPathTest extends App{
  val classPathEntries = javaClassPath
    .split(File.pathSeparator)
    .filterNot(_.isEmpty)
    .map((_, "System Classpath"))

  classPathEntries.foreach(println _)
val uriStr="jar:file:/path/foo.jar!/package/cls.class"
  println("==="+"jar:file:".length+"==="+uriStr.indexOf('!'))
  Some(uriStr.substring("jar:file:".length, uriStr.indexOf('!'))).map(println _)
  //System.getenv()和System.getProperties()的区别
  //System.getenv() 返回系统环境变量值 设置系统环境变量：当前登录用户主目录下的".bashrc"文件中可以设置系统环境变量
  //System.getProperties() 返回Java进程变量值 通过命令行参数的"-D"选项
  val TerminalWidth = if (!sys.env.getOrElse("COLUMNS", "").isEmpty) {
    println("===="+sys.env.getOrElse("COLUMNS", ""))
    sys.env.get("COLUMNS").get.toInt
  } else {
    80
  }
  println(TerminalWidth)
  val width = TerminalWidth / 3
  println(width)
}
