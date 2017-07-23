package ch7


import org.learningconcurrency._
import ch7._




object AtomicHistoryBad extends App {
  import java.util.concurrent.atomic._
  import scala.annotation.tailrec
  import scala.concurrent._
  import ExecutionContext.Implicits.global

  val urls = new AtomicReference[List[String]](Nil)
  val clen = new AtomicInteger(0)

  def addUrl(url: String): Unit = {
    @tailrec def append(): Unit = {
      val oldUrls = urls.get
      if (!urls.compareAndSet(oldUrls, url :: oldUrls)) append()
    }
    append()
    clen.addAndGet(url.length + 1)
  }

  def getUrlArray(): Array[Char] = {
    val array = new Array[Char](clen.get)
    val urlList = urls.get
    for ((character, i) <- urlList.map(_ + "\n").flatten.zipWithIndex) {
      array(i) = character
    }
    array
  }

  Future {
    try { log(s"sending: ${getUrlArray().mkString}") }
    catch { case e: Exception => log(s"problems getting the array $e") }
  }

  Future {
    addUrl("http://scala-lang.org")
    addUrl("https://github.com/scala/scala")
    addUrl("http://www.scala-lang.org/api")
    log("done browsing")
  }

}


















