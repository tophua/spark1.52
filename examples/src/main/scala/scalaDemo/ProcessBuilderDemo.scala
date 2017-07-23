package scalaDemo

/**
  * Created by liush on 17-7-20.
  */
import java.io.File
import java.lang.System._
import java.util
object ProcessBuilderDemo {

  def main(args: Array[String]): Unit = {
    // create a new list of arguments for our process
    val list = Array("notepad.exe", "test.txt")

    // create the process builder
    //ProcessBuilder创建操作系统进程，它提供一种启动和管理进程（也就是应用程序）的方法
    val pb = new ProcessBuilder(list: _*)
    //返回此进程生成器环境的字符串映射视图
    val env: util.Map[String, String] = pb.environment
    env.put("VAR1", "myValue")
    env.remove("OTHERVAR")
    env.put("VAR2", env.get("VAR1") + "suffix")
    // 返回此进程生成器的工作目录。
    pb.directory(new File("."))
    // 通知进程生成器是否合并标准错误和标准输出。
    pb.redirectErrorStream()
    // 使用此进程生成器的属性启动一个新进程。
    val p: Process = pb.start
    // set the command list
    //设置此进程生成器的操作系统程序和参数
    pb.command()
    // print the new command list
    System.out.println("" + pb.command)
  }

}
