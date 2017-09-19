package scalaDemo

import org.apache.spark.deploy.SparkSubmit

/**
  * Created by liush on 17-7-19.
  */
object ThreadJoinTest {
  val thread = new Thread {
    var counter=0
    override def run() = try {
      while (counter <= 10) {
       println("Counter = " + counter + " ")
        counter=counter+1
      }

    } catch {
      case e: Exception =>  throw e
    }
  }

  val thread2 = new Thread {
    var i=0
    override def run() = try {
      while (i <= 10) {
        println("i = " + i + " ")
        i=i+1
      }

    } catch {
      case e: Exception =>  throw e
    }
  }
  def main(args: Array[String]):Unit= {
    thread.start()
    thread.join()

    thread2.start()
    thread2.join()
    /**
      * t1启动后，调用join()方法，直到t1的计数任务结束，才轮到t2启动，然后t2也开始计数任务。可以看到，实例中，两个线程就按着严格的顺序来执行了。
      * 如果t2的执行需要依赖于t1中的完整数据的时候，这种方法就可以很好的确保两个线程的同步性。
      */
  }

}
