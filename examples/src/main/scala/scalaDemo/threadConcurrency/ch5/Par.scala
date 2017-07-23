
package ch5

import org.learningconcurrency._
import ch5._




object ParBasic extends App {
  import scala.collection._
/**
 * 示例使用Vector类,创建了一个包含500万个数字的Vector对象,然后使用Random类随机方式调整该对象排序
 * 然后这段程序比较顺序的max方法和并行max方法的运行时间,这两个方法都用寻找整数集中的最大整数
 */
  val numbers = scala.util.Random.shuffle(Vector.tabulate(5000000)(i => i))

  val seqtime = timed {
    val n = numbers.max
    println(s"largest number $n")
  }
//顺序的max方法运行时间为381.411 毫秒
  log(s"Sequential time $seqtime ms")

  val partime = timed {
    val n = numbers.par.max
    println(s"largest number $n")
  }
//并行的max方法的运行293.74 毫秒
  log(s"Parallel time $partime ms")
}


object ParUid extends App {
  import scala.collection._
  import java.util.concurrent.atomic._
  private val uid = new AtomicLong(0L)
/**
 * 原子变量实现的incrementAndGet方法,使用并行集合,计算数值的唯一标识标
 */
  val seqtime = timed {
    for (i <- 0 until 10000000) uid.incrementAndGet()
  }
  //Sequential time 370.134 ms
  log(s"Sequential time $seqtime ms")

  val partime = timed {
    //for循环中使用了并行集合,当调用并行集合中的foreach方法,集合中的元素会以并发方式被处理
    //这意味着独立的线程同时调用指定的函数,因此必须适当的同步机制.
    for (i <- (0 until 10000000).par) uid.incrementAndGet()
  }
  //Parallel time 714.495 ms  
  log(s"Parallel time $partime ms")
 /**
  * 结论这个并行版本的运行速度甚至更慢,该程序的输出结果表明,顺序foreach方法运行时间为370.134毫秒
  * 而并行foreach方法运行时间为714.495毫秒.
  * 主要原因:多个线程同时调用了原子变更量uid中的incrementAndGet方法,并且同时向一个内存位置执行写入操作
  */
}


object ParGeneric extends App {
  import scala.collection._
  import scala.io.Source

  def findLongestLine(xs: GenSeq[String]): Unit = {
    val line = xs.maxBy(_.length)
    log(s"Longest line - $line")
  }

  val doc = Array.tabulate(1000)(i => "lorem ipsum " * (i % 10))

  findLongestLine(doc)
  findLongestLine(doc.par)

}


object ParConfig extends App {
  import scala.collection._
  import scala.concurrent.forkjoin.ForkJoinPool

  val fjpool = new ForkJoinPool(2)
  val myTaskSupport = new parallel.ForkJoinTaskSupport(fjpool)
  val numbers = scala.util.Random.shuffle(Vector.tabulate(5000000)(i => i))
  val partime = timed {
    val parnumbers = numbers.par
    parnumbers.tasksupport = myTaskSupport
    val n = parnumbers.max
    println(s"largest number $n")
  }
  log(s"Parallel time $partime ms")  
}
/**
 * 假设我们要查明TEXTAREA标记在Html文件中的作用,你可以编写一个程序 ,下载HTML规范文档并搜索第一个出现的TEXTAREA字符串 
 * getHtmlSpec方法通过异步计算操作下载HTML规范,然后返回由HTML规范内容完善的Future对象,之后向该对象添加一个回调函数
 * 一旦获得了HTML规范的内容后,就可以调用Future对象中的indexWhere方法,找到与正则表达式".*TEXTAREA.*"匹配的行
 */

object ParHtmlSpecSearch extends App {
  import scala.concurrent._
  import ExecutionContext.Implicits.global
  import scala.collection._
  import scala.io.Source

  def getHtmlSpec() = Future {
    val specSrc: Source = Source.fromURL("http://www.w3.org/MarkUp/html-spec/html-spec.txt")
    try specSrc.getLines.toArray finally specSrc.close()
  }

  getHtmlSpec() foreach { case specDoc =>
    log(s"Download complete!")
    //GenSeq顺序和并发子类型,这样做使用该程序的顺序和并行版本的运行.
    def search(d: GenSeq[String]) = warmedTimed() {
      d.indexWhere(line => line.matches(".*TEXTAREA.*"))
    }

    val seqtime = search(specDoc)
    //Sequential time 3.711 ms
    log(s"Sequential time $seqtime ms")

    val partime = search(specDoc.par)
    //Parallel time 2.539 ms
    log(s"Parallel time $partime ms")
  }
  /**
   * 运行三次时间分别:3.729 ms,2.419 ms,3.711,2.539 
   * 得出JVM已经达到稳定状态的错误结论,实际上在JVM适当优化该程序之前,我们应该运行该程序更多的次数
   * 
   * 
   */
Thread.sleep(5000)
}

/**
 * 非并行化集合,调用非可并行化集合中的par方法,需要将它们的元素复制到新集合中
 */
object ParNonParallelizableCollections extends App {
  import scala.collection._
 //调用List集合中的par方法时,需要将List集合中的元素复制到Vector集合中
  val list = List.fill(1000000)("")
  val vector = Vector.fill(1000000)("")
  log(s"list conversion time: ${timed(list.par)} ms")
  log(s"vector conversion time: ${timed(vector.par)} ms")
  /**
   * 结论:从顺序集合向并行集合的转换操作本向不是并行,而且有可能成为性能瓶颈
   * main: list conversion time: 206.077 ms
	 * main: vector conversion time: 0.053 ms
   */
}


object ParNonParallelizableOperations extends App {
  import scala.collection._
  import scala.concurrent.ExecutionContext.Implicits.global
  import ParHtmlSpecSearch.getHtmlSpec

  getHtmlSpec() foreach { case specDoc =>
    def allMatches(d: GenSeq[String]) = warmedTimed() {
      val results = d.foldLeft("")((acc, line) => if (line.matches(".*TEXTAREA.*")) s"$acc\n$line" else acc)
      // Note: must use "aggregate" instead of "foldLeft"!
    }

    val seqtime = allMatches(specDoc)
    log(s"Sequential time - $seqtime ms")

    val partime = allMatches(specDoc.par)
    log(s"Parallel time   - $partime ms")
  }
}


object ParNonDeterministicOperation extends App {
  import scala.collection._
  import scala.concurrent.ExecutionContext.Implicits.global
  import ParHtmlSpecSearch.getHtmlSpec

  getHtmlSpec() foreach { case specDoc =>
    val seqresult = specDoc.find(line => line.matches(".*TEXTAREA.*"))
    val parresult = specDoc.par.find(line => line.matches(".*TEXTAREA.*"))
    log(s"Sequential result - $seqresult")
    log(s"Parallel result   - $parresult")
  }
}


object ParNonCommutativeOperator extends App {
  import scala.collection._
  
  val doc = mutable.ArrayBuffer.tabulate(20)(i => s"Page $i, ")
  def test(doc: GenIterable[String]) {
    val seqtext = doc.seq.reduceLeft(_ + _)
    val partext = doc.par.reduce(_ + _)
    log(s"Sequential result - $seqtext\n")
    log(s"Parallel result   - $partext\n")
  }
  test(doc)
  test(doc.toSet)
}


object ParNonAssociativeOperator extends App {
  import scala.collection._

  def test(doc: GenIterable[Int]) {
    val seqtext = doc.seq.reduceLeft(_ - _)
    val partext = doc.par.reduce(_ - _)
    log(s"Sequential result - $seqtext\n")
    log(s"Parallel result   - $partext\n")
  }
  test(0 until 30)
}


object ParMultipleOperators extends App {
  import scala.collection._
  import scala.concurrent.ExecutionContext.Implicits.global
  import ParHtmlSpecSearch.getHtmlSpec

  getHtmlSpec() foreach { case specDoc =>
    val length = specDoc.aggregate(0)(
      (count: Int, line: String) => count + line.length,
      (count1: Int, count2: Int) => count1 + count2
    )
    log(s"Total characters in HTML spec - $length")
  }
}


object ParSideEffectsIncorrect extends App {
  import scala.collection._

  def intSize(a: GenSet[Int], b: GenSet[Int]): Int = {
    var count = 0
    for (x <- a) if (b contains x) count += 1
    count
  }
  val seqres = intSize((0 until 1000).toSet, (0 until 1000 by 4).toSet)
  val parres = intSize((0 until 1000).par.toSet, (0 until 1000 by 4).par.toSet)
  log(s"Sequential result - $seqres")
  log(s"Parallel result   - $parres")
}


object ParSideEffectsCorrect extends App {
  import scala.collection._
  import java.util.concurrent.atomic._

  def intSize(a: GenSet[Int], b: GenSet[Int]): Int = {
    val count = new AtomicInteger(0)
    for (x <- a) if (b contains x) count.incrementAndGet()
    count.get
  }
  val seqres = intSize((0 until 1000).toSet, (0 until 1000 by 4).toSet)
  val parres = intSize((0 until 1000).par.toSet, (0 until 1000 by 4).par.toSet)
  log(s"Sequential result - $seqres")
  log(s"Parallel result   - $parres")
}


object ParMutableWrong extends App {
  import scala.collection._

  val buffer = mutable.ArrayBuffer[Int]() ++= (0 until 250)
  for (x <- buffer.par) buffer += x
  log(buffer.toString)
}






