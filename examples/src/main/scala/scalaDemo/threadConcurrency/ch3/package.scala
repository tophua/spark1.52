package org.learningconcurrency


import scala.concurrent._

package object ch3 {
/**
 * 接收一个代码块主体,创建一个在其run方法中执行该代码块的新线程,运行这个新建线程,然后返回对该线程的引用,
 * 从而使用其他线程能够调用该线程中的run方法
 * ExecutionContext 类为与执行的逻辑线程相关的所有信息提供单个容器
 * 执行全局对象ExecutionContext中的代码块
 */
  def execute(body: =>Unit) = ExecutionContext.global.execute(new Runnable {
    def run() = body
  })

}

