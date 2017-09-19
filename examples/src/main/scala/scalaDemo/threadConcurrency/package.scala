package org

package object learningconcurrency {

  def log(msg: String) {
    //Thread.currentThread().getContextClassLoader,可以获取当前线程的引用,getContextClassLoader用来获取线程的上下文类加载器
    println(s"${Thread.currentThread.getName}: $msg")
  }

}

