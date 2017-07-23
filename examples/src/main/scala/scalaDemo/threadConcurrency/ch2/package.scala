package org.learningconcurrency






package object ch2 {
/**
 * 接收一个代码块主体,创建一个在其run方法中执行该代码块的新线程,运行这个新建线程,然后返回对该线程的引用,
 * 从而使用其他线程能够调用该线程中的join方法
  * 名参传递
 */
  def thread(body: =>Unit): Thread = {
    val t = new Thread {
      override def run() = body
    }
    t.start()
    t
  }

}

