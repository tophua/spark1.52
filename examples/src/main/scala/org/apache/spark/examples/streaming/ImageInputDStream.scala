package org.apache.spark.examples.streaming

import java.io.InputStream
import java.net.Socket
import org.apache.hadoop.io.BytesWritable
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.Logging

/**
 * 自己定义了一个 Scala 类 ImageInputDStream,用于加载 Java的读入图片类
  * Created by wallace on 2016/10/31.
  */
class ImageInputDStream(@transient ssc_ : StreamingContext, host: String, port:
Int, storageLevel: StorageLevel) extends
  ReceiverInputDStream[BytesWritable](ssc_) with Logging {
  override def getReceiver(): Receiver[BytesWritable] = {
    new ImageRecevier(host, port, storageLevel)
  }
}

class ImageRecevier(host: String, port: Int, storageLevel: StorageLevel) extends
  Receiver[BytesWritable](storageLevel) with Logging {
  override def onStart(): Unit = {
    new Thread("Image Socket") {
      setDaemon(true)

      override def run(): Unit = {
        receive()
      }
    }.start()
  }

  def receive(): Unit = {
    var socket: Socket = null
    var in: InputStream = null
    try {
      log.info("Connecting to " + host + ":" + port)
      socket = new Socket(host, port)
      log.info("Connected to " + host + ":" + port)
      in = socket.getInputStream
      val buf = new ArrayBuffer[Byte]()
      var bytes = new Array[Byte](1024)
      var len = 0
      while (-1 < len) {
        len = in.read(bytes)
        if (len > 0) {
          buf ++= bytes
        }
      }
      val bw = new BytesWritable(buf.toArray)
      log.error("byte:::::" + bw.getLength)
      store(bw)
      log.info("Stopped receiving")
      restart("Retrying connecting to " + host + ":" + port)
    } catch {
      case e: java.net.ConnectException =>
        restart("Error connecting to " + host + ":" + port, e)
      case t: Throwable =>
        restart("Error receiving data", t)
    } finally {
      if (in != null) {
        in.close()
      }
      if (socket != null) {
        socket.close()
        log.info("Closed socket to " + host + ":" + port)
      }
    }
  }

  override def onStop(): Unit = {

  }
}
