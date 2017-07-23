package ch5

import org.learningconcurrency._
import ch5._
import ParHtmlSpecSearch._
import scala.io.Source
import scala.concurrent.Future



/**
 * 并发不正常
 */
object ConcurrentWrong extends App {
  import scala.collection._
  import scala.concurrent.ExecutionContext.Implicits.global
 

  
  def getUrlSpec(): Future[Seq[String]] = Future {
    val f = Source.fromURL("http://www.w3.org/Addressing/URL/url-spec.txt")
    try{
      f.getLines.toList 
    }finally{
      f.close()
    }
  }
  def intersection(a: GenSet[String], b: GenSet[String]): GenSet[String] = {
    val result = new mutable.HashSet[String]
    for (x <- a.par) if (b contains x) result.add(x)
    result
  }

  val ifut = for {
    htmlSpec <- getHtmlSpec()
    urlSpec <- getUrlSpec()
  } yield {
    val htmlWords = htmlSpec.mkString.split("\\s+").toSet
    val urlWords = urlSpec.mkString.split("\\s+").toSet
    intersection(htmlWords, urlWords)
  }

  ifut onComplete {
    case t => log(s"Result: $t")
  }
}
/**
 * 并发集合
 * 
 */

object ConcurrentCollections extends App {
  import java.util.concurrent.ConcurrentSkipListSet
  import scala.collection._
  import scala.collection.convert.decorateAsScala._
  import scala.concurrent.ExecutionContext.Implicits.global
  import ParHtmlSpecSearch.getHtmlSpec
 
 
  
  def getUrlSpec(): Future[Seq[String]] = Future {
    val f = Source.fromURL("http://www.w3.org/Addressing/URL/url-spec.txt")
    //f.close() 关闭流的连接
    try f.getLines.toList finally f.close()
  }
  
  def intersection(a: GenSet[String], b: GenSet[String]): GenSet[String] = {
    val skiplist = new ConcurrentSkipListSet[String]
    for (x <- a.par) if (b contains x) skiplist.add(x)
    val result: Set[String] = skiplist.asScala
    result
  }

  val ifut = for {
    htmlSpec <- getHtmlSpec()
    urlSpec <- getUrlSpec()
  } yield {
    val htmlWords = htmlSpec.mkString.split("\\s+").toSet
    val urlWords = urlSpec.mkString.split("\\s+").toSet
    intersection(htmlWords, urlWords)
  }

  ifut foreach { case i =>
    log(s"intersection = $i")
  }

}


object ConcurrentCollectionsBad extends App {
  import java.util.concurrent.ConcurrentSkipListSet
  import scala.collection._
  import scala.collection.parallel._

  def toPar[T](c: ConcurrentSkipListSet[T]): ParSet[T] = ???

  val c = new ConcurrentSkipListSet[Int]
  for (i <- 0 until 100) c.add(i)
  
  for (x <- toPar(c)) c.add(x) // bad
}


object ConcurrentTrieMap extends App {
  import scala.collection._

  val cache = new concurrent.TrieMap[Int, String]()
  for (i <- 0 until 100) cache(i) = i.toString

  for ((number, string) <- cache.par) cache(-number) = s"-$string"

  log(s"cache - ${cache.keys.toList.sorted}")
}



