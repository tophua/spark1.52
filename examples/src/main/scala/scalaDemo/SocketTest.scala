package scalaDemo

import java.net.Socket

object SocketTest {
   def main(args: Array[String]) {
    val host="192.168.10.198"
    val port=2389
    val socket = new Socket(host, port)
  println(socket.getOutputStream)
    println(socket.getKeepAlive)
  }
}