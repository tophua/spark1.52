package scalaDemo

import java.io.File

import scala.io.Source
/**
 * 隐式转换
 * Created by zhiwang on 2015/7/21.
 */

class RichFile(val file: File) {
  def read() = Source.fromFile(file.getPath()).mkString
}
object Context {
  //File -> RichFile,必须有implicit关键字
  implicit def file2RichFile(file: File) = new RichFile(file)
}
/**
 代码的执行过程如下： 
1. 调用File 的read 方法 
2. 当编译器发现File类没有read 方法时,不是直接报错,而是执行第三步 
3. 检查当前作用域中是否有接受File的 implicit 方法,如没有直接报错,如有,执行第4步 
4. 将File作为参数实例化RichFile,再检查是否有read 方法,如没有直接报错 
5. 执行read方法
整个执行过程中,需要特别注意的是, 作用域
**/
object ImplicitConvertion {
  def main(args: Array[String]) {
    //在当前作用域引入隐式转换
    import Context.file2RichFile
    //文件只能英文,不能包括中文
    //File本身是没有read方法的,需要隐式转换为自定义的RichFile
    println(new File("c:\\aa.txt").read())
  }
}
