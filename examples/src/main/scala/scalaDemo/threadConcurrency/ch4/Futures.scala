package ch4

import org.learningconcurrency._
/**
 * Future 它是一个容器类型，代表一个代码最终会返回的T类型结果。不过，代码可能会出错或执行超时，
 * 所以当Future完成时，它有可能完全没有被成功执行，这时它会代表一个异常
 *  Future 表示一个可能还没有实际完成的异步任务的结果,
 *  针对这个结果可以添加 Callback 以便在任务执行成功或失败后做出对应的操作
 */
object FuturesComputation extends App {
  /**
   * Computation计算
   *  Futures 执行计算
   * 1,首先引入global执行上下文,可以确保在全局上下文中执行Future计算
   * 调用log方法的次序不是确定,后跟代码块的Futures单例对象,是为调用Apply方法而添加语法糖
   * 
   */
  import scala.concurrent._
  import ExecutionContext.Implicits.global

  Future {
    log(s"the future is here")
  }

  log(s"the future is coming")

}

/**
 *  通过Futures计算,使用Source.fromFile对象读取build.sbt文件内容
 * 1,首先引入global执行上下文,可以确保在全局上下文中执行Future计算
 * 
 * 
 */
object FuturesDataType extends App {
  import scala.concurrent._
  import ExecutionContext.Implicits.global
  import scala.io.Source
 //通过Futures计算,使用Source.fromFile对象读取build.sbt文件内容
  val buildFile: Future[String] = Future {
    val f = Source.fromFile("build.sbt")
    try f.getLines.mkString("\n") finally f.close()
  }

  log(s"started reading build file asynchronously")
  /**
   * main线程会调用Future对象中的isCompleted方法,该Future对象为通过执行Future计算获得buildFile对象
   * 读取build.sbt文件的操作很可能很快完成,因此isCompleted方法会返回false,
   * 过250毫秒后,main线程会再次调用isCompleted方法会返回true,
   * 最后main线程会调用value方法,该方法会返回build.sb文件的内容
   * 
   */
  log(s"status: ${buildFile.isCompleted}")//当异步操作完成了且返回了true值,是success
  Thread.sleep(250)
  log(s"status: ${buildFile.isCompleted}")//当异步操作没有完成返回false
  log(s"status: ${buildFile.value}")//还回值

}
/**
 * Futures对象的回调函数
 * 从w3网中查找出所有单词telnet.
 */

object FuturesCallbacks extends App {
  import scala.concurrent._
  import ExecutionContext.Implicits.global
  import scala.io.Source
/**
 * 从w3网站获得url规范的文档,使用Source对象存储该文件的内容,并使用getUrlSpec文件中的Future对象
 * 以异步方式执行Http请求操作,getUrlSpec方法会先调用fromURL获取含有文本文档的Source对象,然后会调用
 * getLines方法获取文档中的行列表.
 */
  
  def getUrlSpec(): Future[Seq[String]] = Future {
    //以异步方式执行http请求操作
    val f = Source.fromURL("http://www.w3.org/Addressing/URL/url-spec.txt")
    try {
      f.getLines.toList
      }finally{
      f.close()
     }
  }

  val urlSpec: Future[Seq[String]] = getUrlSpec()
/**
 * 要在Future对象urlSpec中找到包含有单词telnet的行,可以使用find方法
 * 该方法将行的列表和需要搜索的单词接收为参数,并且返回含有匹配内容的字符串.
 * 接收一个Seq类型的参数,而urlSpec对象返回Future[Seq[String]],因此无法将Future对象urlSpec
 * 直接发送给find方法,而且在程序调用find方法时,该Future很可能还无法使用.
 */
  def find(lines: Seq[String], word: String) = lines.zipWithIndex.collect {
    //zipWithIndex， 返回对偶列表，第二个组成部分是元素下标  
    case (line, n) if line.contains(word) => (n, line)
  }.mkString("\n")//此迭代器转换为字符串
/**
 * 我们使用foreach方法为这个Future添加一个回调函数,注意onSuccess方法与foreach方法等价,但onSuccess方法可能
 * 会在scala 2.11之后被弃用,foreach方法接收偏函数作为其参数 
 */
  urlSpec.foreach {//foreach接收一个偏函数
    case lines => log(s"Found occurrences of 'telnet'\n${find(lines, "telnet")}\n")
  }
  /**
  * Thread.sleep
  * 此处的要点是:添加回调函数是非阻塞操作,回调用函数注册后,main线程中用的log语句会立即执行
 	* 但是执行回调函数中log语句的时间可以晚得多
 	* 在Future对象被完善后,无须立刻调用回调参数,大多数执行上下文通过调用任务,以异步方式处理回调函数
   */
   Thread.sleep(2000)//添加此方法如果不添加不异步调用不显示信息
   log("callbacks registered, continuing with other work")

/**
 * ForkJoinPool-1-worker-5: Found occurrences of 'telnet'
 * (207,  telnet , rlogin and tn3270 )
 * (745,                         nntpaddress | prosperoaddress | telnetaddress)
 * (806,  telnetaddress           t e l n e t : / / login )
 * (931,   for a given protocol (for example,  CR and LF characters for telnet)
   ForkJoinPool-1-worker-5: Found occurrences of 'password'
 * (107,                         servers). The password, is present, follows)
 * (109,                         the user name and optional password are)
 * (111,                         user of user name and passwords which are)
 * (222,      User name and password)
 * (225,   password for those systems which do not use the anonymous FTP)
 * (226,   convention. The default, however, if no user or password is)
 * (234,   is "anonymous" and the password the user's Internet-style mail)
 * (240,   currently vary in their treatment of the anonymous password.  )
 * (816,  login                   [ user [ : password ] @ ] hostport )
 * (844,  password                alphanum2 [ password ] )
 * (938,   The use of URLs containing passwords is clearly unwise. )
 */
   /**
    * Future对象添加多个回调函数,如果我们还想在文档中找出所有单词password,可以再添加一个回调函数
    */
  urlSpec.foreach {
    lines => log(s"Found occurrences of 'password'\n${find(lines, "password")}\n")
  }
  Thread.sleep(1000)

  log("callbacks installed, continuing with other work")
  

}

/**
 * Failure计算和异常处理
 */
object FuturesFailure extends App {
  import scala.concurrent._
  import ExecutionContext.Implicits.global
  import scala.io.Source
/**
 * 当完善Future对象的操作被执行后,有可能成功Future对象,也有可能失败Future对象, 
 */
  val urlSpec: Future[String] = Future {
    /**
     * 向一个非法的URL发送了http请求,fromURL方法抛出了一个异常,而且Future对象urlSpec的操作失败了.
     */
    Source.fromURL("http://www.w3.org/non-existent-url-spec.txt").mkString
  }
  /**
   * foreach方法会接收处理成功完善的Future对象的回调函数,
   * failed方法会接收处理接失败情况的回调函数,返回Future[Throwable]对象,该对象代表Future失败情况的异常
   * 将failed方法与foreach一起使用可以访问异常
   */

  urlSpec.failed.foreach {    
    case t => {
      log(s"exception occurred - $t")     
    }    
  }
  Thread.sleep(2000)

}

/**
 * 为了使代码简洁,有时候要在同一个回调函数中处理成功和失败的情况
 * 使用Try类型,有两个子类型:Success类型用于表示成功执行操作结果
 * 												Failure类型用于表示执行失败的异常对象
 * 我们可以使用模式匹配功能确定Try对象是那种子类型
 */
object FuturesExceptions extends App {
  import scala.concurrent._
  import ExecutionContext.Implicits.global
  import scala.io.Source

  val file = Future { Source.fromFile(".gitignore-SAMPLE").getLines.mkString("\n") }

  //成功回调
  file.foreach {
    text => log(text)
  }
  //失败回调
  file.failed foreach {//异常处理,抛出异常类型FileNotFoundException
    case fnfe: java.io.FileNotFoundException => log(s"Cannot find file - $fnfe")
    case t => log(s"Failed due to $t")
  }
  import scala.util.{Try, Success, Failure}

  file.onComplete {//回调函数时,我们使用提供Success[T]值和Failure[T]值匹配的偏函数
    case Success(text) => log(text)
    case Failure(t) => log(s"Failed due to $t")
  }
  Thread.sleep(2000)

}
/**
 * 使用Try类型,有两个子类型:Success类型用于表示成功执行操作结果
 * 												Failure类型用于表示执行失败的异常对象
 * 我们可以使用模式匹配功能确定Try对象是那种子类型
 */

object FuturesTry extends App {
  import scala.util._
/**
 * Try[String]对象是通过同步方式使用的不可变对象,与Future对象不同,从被创建的那一刻起,Try[String]就会含有一个值
 * 或异常,与其说像Future对象,倒不如说它更像集合.
 */
  val threadName: Try[String] = Try(Thread.currentThread.getName)
  val someText: Try[String] = Try("Try objects are created synchronously")
  val message: Try[String] = for {
    tn <- threadName
    st <- someText
  } yield s"$st, t = $tn"

  message match {
    case Success(msg) => log(msg)
    case Failure(error) => log(s"There should be no $error here.")
  }

}

/**
 * 致命异常
 * 前面介绍Future对象会失效的情况,Future无法捕捉的Throwable对象,
 */
object FuturesNonFatal extends App {
  import scala.concurrent._
  import ExecutionContext.Implicits.global

  val f = Future { throw new InterruptedException }
  val g = Future { throw new IllegalArgumentException }
  f.failed foreach { case t => log(s"error - $t") }
  g.failed foreach { case t => log(s"error - $t") }
  Thread.sleep(2000)
}
/**
 * Future对象中的函数组合
 * 引入Future对象后就将阻塞线程的责任,从API上转移到了调用者线程身上,
 */

object FuturesClumsyCallback extends App {
  import scala.concurrent._
  import ExecutionContext.Implicits.global
  import org.apache.commons.io.FileUtils._
  import java.io._
  import scala.io.Source
  import scala.collection.convert.decorateAsScala._
/**
 * blacklistFile该方法会读取文件.gitignore内容,
 * 过虑掉空白行和所有以#开头的注释行,findFiles方法递归查找符合文件名(gitignore)
 * 返回的Future对象,最终会含有一个字符串列表,代表SBT文件存储目录中由scala创建的文件.
 * 
 * 异步操作读取文件内容
 */
  def blacklistFile(filename: String) = Future {
    val lines = Source.fromFile(filename).getLines
    lines.filter(x => !x.startsWith("#") && !x.isEmpty()).toList
  }
  
  /**
   * 异步读取文件的内容,以异步方式扫描项目目录中的所有文件并对它们执行匹配操作
   * 
   * 将格式列表提交给该方法后,该方法可以找到当前目录中符合这些格式的所有文件
   */
  def findFiles(patterns: List[String]): List[String] = {
    val root = new File(".")
    //println(root.getAbsolutePath+"|||||"+root.getCanonicalPath)
    for {
      //iterateFiles方法开源IO包中,会返回这些项目文件的Java迭代器,因此可以通过调用asScala方法将
      //之转换为Scala迭代器,然后获得所有匹配的文件路径
      f <- iterateFiles(root, null, true).asScala.toList
      pat <- patterns
      abspat = root.getCanonicalPath + File.separator + pat
      if f.getCanonicalPath.contains(abspat)
    } yield
    {
      println(">>>>>>>."+f.getCanonicalPath)
      f.getCanonicalPath
    }
    
  }
  /**
   * 通过Future对象中的函数组合可以在for推导语句中使用Future对象,而且通常比使用回调函数直观.
   * 使用foreach可以避开彻底阻塞的情况
   * 
   */
//val lines = Source.fromFile(".gitignore").getLines
//  lines.filter(x => !x.startsWith("#") && !x.isEmpty()).foreach { x => println(">>>>>>>>>>"+x) }

  blacklistFile(".gitignore").foreach {
    case lines =>      
      val files = findFiles(lines)
      files.foreach {  x => println("|||||||||"+x) }
      log(s"matches: ${files.mkString("\n")}")
  }
  
  /**
   * 可以使用Map方式简化操作,Map方法接收函数f,并返回新的Future对象,
   */
  
  def blacklistFiles(filename: String):Future[List[String]]=blacklistFile(filename).map(patterns => findFiles(patterns))
  blacklistFiles(".gitignore").foreach { x =>  log(s"matches: ${x.mkString("\n")}") }
   Thread.sleep(2000)
   //System.exit(0);
}
/**
 * 使用Map方式简化操作,Map方法接收函数f,并返回新的Future对象,
 * 我们使用Future对象,从build.sbt文件中获最长的一行,
 */

object FuturesMap extends App {
  import scala.concurrent._
  import ExecutionContext.Implicits.global
  import scala.io.Source
  import scala.util.Success
//从硬盘读取该文件
  val buildFile = Future { Source.fromFile("build.sbt").getLines }
 //异步读取文件
  val gitignoreFile = Future { Source.fromFile(".gitignore-SAMPLE").getLines }
/**
 * Future.map方法可以将一个Future对象中的值与另一个Future对象中的值对应起来,该方法不会阻塞线程,会立即回返回Future对象
 * 当初始的Future对象通过某个值完善后,被返回Future[S]对象最终会被f(x)方法完善
 */
  val longestBuildLine = buildFile.map(lines => lines.maxBy(_.length))//获最长的一行  
  longestBuildLine.onComplete {
    case Success(line) => log(s"the longest line is '$line'")
  }
  //使用for推导语句,异常处理
  val longestGitignoreLine = for (lines <- gitignoreFile) yield lines.maxBy(_.length)
  longestGitignoreLine.failed.foreach {
    case t => log(s"no longest line, because ${t.getMessage}")
  }
   Thread.sleep(2000)
}
/**
 * Raw 未加工的; 无经验的
 */

object FuturesFlatMapRaw extends App {
  import scala.concurrent._
  import ExecutionContext.Implicits.global
  import scala.io.Source
//异步方式获取网络文件
  val netiquette = Future { Source.fromURL("http://www.ietf.org/rfc/rfc1855.txt").mkString }
  //异步方式获取标题规范
  val urlSpec = Future { Source.fromURL("http://www.w3.org/Addressing/URL/url-spec.txt").mkString }
  //使用flatMap和map组合
  val answer = netiquette.flatMap { nettext =>
    urlSpec.map { urltext =>
      "First, read this: " + nettext + ". Now, try this: " + urltext
    }
  }
  answer foreach {
    case contents => log(contents)
  }
   Thread.sleep(2000)
  
}


object FuturesFlatMap extends App {
  import scala.concurrent._
  import ExecutionContext.Implicits.global
  import scala.io.Source
/**
 * 通过隐含方式在for推导语句中使用flatMap方法
 */
  val netiquette = Future { Source.fromURL("http://www.ietf.org/rfc/rfc1855.txt").mkString }
  val urlSpec = Future { Source.fromURL("http://www.w3.org/Addressing/URL/url-spec.txt").mkString }
  val answer = for {
    nettext <- netiquette
    urltext <- urlSpec
  } yield {
    "First of all, read this: " + nettext + " Once you're done, try this: " + urltext
  }
/**
 * 经过编译后,该for推导语句就会变为使用flatMap方法的代码,这样使用我们的工作变得简单多了,这段程序的可读性非常高.
 * 对于Future对象netiquette中的nettext值和Future对象urltext值来说,answer是通过将nettext和urltext值连接起来生成新
 * 的Future对象
 */
  answer.foreach {
    case contents => log(contents)
  }
  Thread.sleep(2000)
}


object FuturesDifferentFlatMap extends App {
  import scala.concurrent._
  import ExecutionContext.Implicits.global
  import scala.io.Source
/**
 * for推导语句nettext是从第一个Future对象被完善后,完善第二个Future对象的操作才会被执行,
 * 当异步方式使用nettext值计算第二个Future对象的完善时,这种模式才有意义
 */
  val answer = for {
    nettext <- Future { Source.fromURL("http://www.ietf.org/rfc/rfc1855.txt").mkString }
    urltext <- Future { Source.fromURL("http://www.w3.org/Addressing/URL/url-spec.txt").mkString }
  } yield {
    "First of all, read this: " + nettext + " Once you're done, try this: " + urltext
  }

  answer foreach {
    case contents => log(contents)
  }

}


object FuturesRecover extends App {
  import scala.concurrent._
  import ExecutionContext.Implicits.global
  import scala.io.Source
/**
 * Future提供出错时显示默认消息
 */
  val netiquetteUrl = "http://www.ietf.org/rfc/rfc1855.doc"
  val netiquette = Future { Source.fromURL(netiquetteUrl).mkString } recover {
    case f: java.io.FileNotFoundException =>
      "Dear boss, thank you for your e-mail." +
      "You might be interested to know that ftp links " +
      "can also point to regular files we keep on our servers."
  }

  netiquette foreach {
    case contents => log(contents)
  }

}


object FuturesReduce extends App {
  import scala.concurrent._
  import ExecutionContext.Implicits.global

  val squares = for (i <- 0 until 10) yield Future { i * i }
  val sumOfSquares = Future.reduce(squares)(_ + _)

  sumOfSquares foreach {
    case sum => log(s"Sum of squares = $sum")
  }
}



