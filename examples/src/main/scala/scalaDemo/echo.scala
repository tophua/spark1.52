package scalaDemo

import org.apache.spark.rdd.RDD

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
}
