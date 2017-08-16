package org.apache.spark.network.nio

import java.nio.ByteBuffer

import org.apache.spark.{SecurityManager, SparkConf}
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Created by liush on 17-8-16.
  */
object ConnectionManagerTest{
  import scala.concurrent.ExecutionContext.Implicits.global

  def main(args: Array[String]) {
    val conf = new SparkConf
    val manager = new ConnectionManager(9999, conf, new SecurityManager(conf))
    manager.onReceiveMessage((msg: Message, id: ConnectionManagerId) => {
      // scalastyle:off println
      println("Received [" + msg + "] from [" + id + "]")
      // scalastyle:on println
      None
    })

     testSequentialSending(manager)
    System.gc()




/*    testContinuousSending(manager)
    System.gc()
    testParallelSending(manager)
    System.gc()
    testParallelDecreasingSending(manager)
    System.gc()*/

  }

  // scalastyle:off println
  def testSequentialSending(manager: ConnectionManager) {
    println("--------------------------")
    println("Sequential Sending")//顺序
    println("--------------------------")
    //10485760字节是10MB
    val size = 10 * 1024 * 1024
    val count = 10
    //ByteBuffer.allocate在能够读和写之前,必须有一个缓冲区,用静态方法 allocate() 来分配缓冲区
    val buffer = ByteBuffer.allocate(size).put(Array.tabulate[Byte](size)(x => x.toByte))
    buffer.flip

    (0 until count).map(i => {
      val bufferMessage = Message.createBufferMessage(buffer.duplicate)
      //Await.result或者Await.ready会导致当前线程被阻塞,并等待actor通过它的应答来完成Future
      Await.result(manager.sendMessageReliably(manager.id, bufferMessage), Duration.Inf)
    })
    println("--------------------------")
    println()
  }

  def testParallelSending(manager: ConnectionManager) {
    println("--------------------------")
    println("Parallel Sending")//顺序
    println("--------------------------")
    //10485760字节是10MB
    val size = 10 * 1024 * 1024
    val count = 10
    //ByteBuffer.allocate在能够读和写之前,必须有一个缓冲区,用静态方法 allocate() 来分配缓冲区
    val buffer = ByteBuffer.allocate(size).put(Array.tabulate[Byte](size)(x => x.toByte))
    buffer.flip

    val startTime = System.currentTimeMillis
    (0 until count).map(i => {
      val bufferMessage = Message.createBufferMessage(buffer.duplicate)
      manager.sendMessageReliably(manager.id, bufferMessage)
    }).foreach(f => {
      f.onFailure {
        case e => println("Failed due to " + e)
      }
      Await.ready(f, 1 second)
    })
    val finishTime = System.currentTimeMillis

    val mb = size * count / 1024.0 / 1024.0
    val ms = finishTime - startTime
    val tput = mb * 1000.0 / ms
    println("--------------------------")
    println("Started at " + startTime + ", finished at " + finishTime)
    println("Sent " + count + " messages of size " + size + " in " + ms + " ms " +
      "(" + tput + " MB/s)")
    println("--------------------------")
    println()
  }

  def testParallelDecreasingSending(manager: ConnectionManager) {
    println("--------------------------")
    println("Parallel Decreasing Sending")//平行减少发送
    println("--------------------------")
    val size = 10 * 1024 * 1024
    val count = 10
    val buffers = Array.tabulate(count) { i =>
      val bufferLen = size * (i + 1)
      val bufferContent = Array.tabulate[Byte](bufferLen)(x => x.toByte)
      //ByteBuffer.allocate在能够读和写之前,必须有一个缓冲区,用静态方法 allocate() 来分配缓冲区
      ByteBuffer.allocate(bufferLen).put(bufferContent)
    }
    buffers.foreach(_.flip)
    val mb = buffers.map(_.remaining).reduceLeft(_ + _) / 1024.0 / 1024.0

    val startTime = System.currentTimeMillis
    (0 until count).map(i => {
      val bufferMessage = Message.createBufferMessage(buffers(count - 1 - i).duplicate)
      manager.sendMessageReliably(manager.id, bufferMessage)
    }).foreach(f => {
      f.onFailure {
        case e => println("Failed due to " + e)
      }
      Await.ready(f, 1 second)
    })
    val finishTime = System.currentTimeMillis

    val ms = finishTime - startTime
    val tput = mb * 1000.0 / ms
    println("--------------------------")
    /* println("Started at " + startTime + ", finished at " + finishTime) */
    println("Sent " + mb + " MB in " + ms + " ms (" + tput + " MB/s)")
    println("--------------------------")
    println()
  }

  def testContinuousSending(manager: ConnectionManager) {
    println("--------------------------")
    println("Continuous Sending")//连续发送
    println("--------------------------")
    val size = 10 * 1024 * 1024
    val count = 10
    //ByteBuffer.allocate在能够读和写之前,必须有一个缓冲区,用静态方法 allocate() 来分配缓冲区
    val buffer = ByteBuffer.allocate(size).put(Array.tabulate[Byte](size)(x => x.toByte))
    buffer.flip

    val startTime = System.currentTimeMillis
    while(true) {
      (0 until count).map(i => {
        val bufferMessage = Message.createBufferMessage(buffer.duplicate)
        manager.sendMessageReliably(manager.id, bufferMessage)
      }).foreach(f => {
        f.onFailure {
          case e => println("Failed due to " + e)
        }
        Await.ready(f, 1 second)
      })
      val finishTime = System.currentTimeMillis
      Thread.sleep(1000)
      val mb = size * count / 1024.0 / 1024.0
      val ms = finishTime - startTime
      val tput = mb * 1000.0 / ms
      println("Sent " + mb + " MB in " + ms + " ms (" + tput + " MB/s)")
      println("--------------------------")
      println()
    }
  }


}
