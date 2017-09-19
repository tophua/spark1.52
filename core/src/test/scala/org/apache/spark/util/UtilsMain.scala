package org.apache.spark.util

object UtilsMain {
  def main(args: Array[String]): Unit = {
    //ActorSystem是重量级的对象,会创建1...N个线程,所以一个application一个ActorSystem
   println("localhostName:"+Utils.localHostName())
    //println(Utils.nonNegativeMod(8,10))
    println(Utils.nonNegativeMod(2,15))
    println(Utils.nonNegativeMod(5,2))
    println(Utils.nonNegativeMod(0,1))
    println(Utils.nonNegativeMod(23,5))
  }
}