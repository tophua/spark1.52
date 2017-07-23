package sparkDemo

import java.net.{ConnectException, InetAddress, Socket, URI}

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success, Try}
/**
  * 测试是否连接到服务器检测
  */
object SparkConnectCheckTest {
  def main(args: Array[String]) {
    val b39=isSparkOnline(URI.create("spark://192.168.0.40:8088"))
    val b27=isSparkOnline(URI.create("spark://192.168.0.13:8088"))
    println("Connect-> b39:"+b39+"\t b27:"+b27)
    //local[*]
    SparkUtility.Connect(SparkUtility.Conf("spark://appdept3:8088", "appname")) match {
      case Success(sc) => println("Success")
      case Failure(f) => println("Failure")
    }
  }

  /**
    * 测试TCP连接正常
    */
  def isSparkOnline(masterLocation: URI): Boolean = {
    try {
      val host = InetAddress.getByName(masterLocation.getHost)
      val socket = new Socket(host, masterLocation.getPort)
      socket.close()
      true
    } catch {
      case ex: ConnectException =>false
    }
  }
}
/**
  * Spark 环境连接正常
  */
object SparkUtility {
  def Conf(url: String, app: String): SparkConf =
    new SparkConf().setMaster(url).setAppName(app)

  def Connect(conf: SparkConf): Try[SparkContext] =
    Try(new SparkContext(conf))
}