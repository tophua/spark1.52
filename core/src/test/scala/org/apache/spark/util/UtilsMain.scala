package org.apache.spark.util

object UtilsMain {
  def main(args: Array[String]): Unit = {
    //ActorSystem是重量级的对象,会创建1...N个线程,所以一个application一个ActorSystem
   println("localhostName:"+Utils.localHostName())
  }
}