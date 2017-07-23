package scalaDemo

import java.io._
import java.util.Date

/**
  * Created by liush on 17-7-14.
  * Scala函数按名称调用
  * 通常情况下，函数的参数是传值参数;即，参数的值在它被传递给函数之前被确定。但是，如果我们需要编写一个接收参数不希望马上计算，直到调用函数内的表达式。
  * 对于这种情况，Scala提供按名称参数调用函数。
  * 按名称调用机制传递一个代码块给被调用者并且每次被调用方传接入参数，代码块被执行，值被计算。
  */
object BaseScala {
  def main(args: Array[String]):Unit= {
    printInt(b=5, a=7)
    delayed(time())
    printStrings("Hello", "Scala", "Python")
    println("Returned Value : " + addInt())
    println("高阶函数:"+ apply( layout, 10) )
    //Scala匿名函数
    var inc = (x:Int) => x+1
    //变量inc现在可以使用以通常的方式的函数：
    var x = inc(7)-1
    println("一个参数匿名函数:"+(inc(7)-1))
    //用多个参数定义的函数如下：
    var mul = (x: Int, y: Int) => x*y
    println("多个参数匿名函数:"+mul(3, 4))
    //无参数定义函数如下：
    var userDir = () => { System.getProperty("user.dir") }
    println("无参数匿名函数:"+ userDir() )

    val str1:String = "Hello, "
    val str2:String = "Scala!"
    //Scala柯里函数
    println( "str1 + str2 = " +  strcat(str1)(str2) )

    //Scala嵌套函数
    println( factorial(0) )
    println( factorial(1) )
    println( factorial(2) )
    println( factorial(3) )


    val date = new Date
    //Scala部分应用函数
    //如果只发送了几个参数，会得到一个部分函数应用。也可以绑定一些参数，剩下其余的稍后再填补上

    /**
      * log()方法有两个参数：date和message。如果想多次调用这个方法，如：date 使用不同的值，而 message 的值相同。
      * 我们可以消除部分参数传递给 log() 方法，因为传递 date 在每个调用都可能会有干扰。要做到这一点，
      * 我们首先绑定一个值到 date 参数，并把下划线放在其位置第二个参数之后
      */
    val logWithDateBound = log(date, _ : String)

    logWithDateBound("message1" )
    Thread.sleep(1000)
    logWithDateBound("message2" )
    Thread.sleep(1000)
    logWithDateBound("message3" )


    ///遍历元组
    //Tuple.productIterator()方法来遍历一个元组的所有元素
    val t = (4,3,2,1)

    t.productIterator.foreach{ i =>println("Value = " + i )}

    /**
      * Scala的Option[T]是容器对于给定的类型的零个或一个元件。Option[T]可以是一些[T]或None对象，它代表一个缺失值。
      * Scala映射get方法产生，如果给定的键没有在映射定义的一些(值)，如果对应于给定键的值已经找到，或None。
      * 选项Option类型常用于Scala程序，可以比较这对null值Java可用这表明没有任何值
      */
    val capitals = Map("France" -> "Paris", "Japan" -> "Tokyo")
    println("capitals.get( 'France' ) : " +  capitals.get( "France" ))
    println("capitals.get( 'India' ) : " +  capitals.get( "India" ))
    //區別Scala的Option
    println("show(capitals.get( 'Japan')) : " +show(capitals.get( "Japan")) )
    println("show(capitals.get( 'India')) : " +show(capitals.get( "India")) )

    val a:Option[Int] = Some(5)
    val b:Option[Int] = None
    //getOrElse()来访问值或使用默认值
    println("a.getOrElse(0): " + a.getOrElse(0) )
    println("b.getOrElse(10): " + b.getOrElse(10) )
    //isEmpty()检查该选项是否为 None
    println("a.isEmpty: " + a.isEmpty )
    println("b.isEmpty: " + b.isEmpty )


    //迭代器不是集合，而是一种由一个访问的集合之一的元素
    //调用 it.next()将返回迭代器的下一个元素
    //是否有更多的元素使用迭代器的it.hasNext方法返回
    val it = Iterator("a", "number", "of", "words")

    while (it.hasNext){
      println(it.next())
    }

    val ita = Iterator(20,40,2,50,69, 90)
    val itb = Iterator(20,40,2,50,69, 90)
  //查找最大元素。迭代器就是在这个方法返回后结束
    println("Maximum valued element " + ita.max )
    //查找最小元素，迭代器就是在这个方法返回后结束
    println("Minimum valued element " + itb.min )

    //Scala模式匹配
    println("匹配一个整数值:"+matchTestValue(3))

    println("不同类型的模式值:"+matchTest("two"))
    println("不同类型的模式值:"+matchTest("test"))
    println("不同类型的模式值:"+matchTest(1))


    //case classes是用于模式匹配与case 表达式指定类
    val alice = new Person("Alice", 25)
    val bob = new Person("Bob", 32)
    val charlie = new Person("Charlie", 32)
    //case classes是用于模式匹配与case 表达式指定类
    for (person <- List(alice, bob, charlie)) {
      person match {
        case Person("Alice", 25) => println("Hi Alice!")
        case Person("Bob", 32) => println("Hi Bob!")
        case Person(name, age) =>
          println("Age: " + age + " year, name: " + name + "?")
      }
    }


    //Scala正则表达式
    //Scala支持通过Regex类的scala.util.matching封装正则表达式
    //我们创建一个字符串，并调用r()方法就可以了。Scala中字符串隐式转换为一个RichString并调用该方法来获得正则表达式的一个实例
    val pattern = "Scala".r
    val str = "Scala is Scalable and cool"
    //从Scala中一个语句中找出单词：
    //找到第一个正则表达式匹配，只需调用findFirstIn()方法
    println(pattern findFirstIn str)



    //Scala异常处理
    //Scala中try/catch在一个单独的块捕捉任何异常，然后使用case块进行模式匹配
    try {
      val f = new FileReader("input.txt")
    } catch {
      case ex: FileNotFoundException =>{
        println("Missing file exception")
      }
      case ex: IOException => {
        println("IO Exception")
      }
    }finally {
      println("Exiting finally...")
    }


    //Scala文件I/O
    //写入文件的一个例子：
    val writer = new PrintWriter(new File("test.txt" ))

    writer.write("Hello Scala")
    writer.close()

    //从屏幕读取一行
    print("Please enter your input : " )
   // val line = Console.readLine()

   // println("Thanks, you just typed: " + line)

  }
  // case class, empty one.
  case class Person(name: String, age: Int)
  //它匹配针对不同类型的模式值：
  def matchTest(x: Any): Any = x match {
    case 1 => "one"
    case "two" => 2
    case y: Int => "scala.Int"
    case _ => "many"
  }
//匹配一个整数值
  def matchTestValue(x: Int): String = x match {
    case 1 => "one"
    case 2 => "two"
    case _ => "many"
  }

  def time() = {
    println("Getting time in nano seconds")
    System.nanoTime
  }
  //我们声明delayed方法,它通过=>符号定意变量的名称和反回类型，需要一个按名称调用参数。
  def delayed( t: => Long ) = {
    println("In delayed method")
    println("Param: " + t)
    //delayed打印一个与其消息的值,最后delayed方法返回 t,
    t
  }
  //使用命名参数
  def printInt( a:Int, b:Int ) = {
    println("Value of a : " + a );
    println("Value of b : " + b );
  }
//使用可变参数
def printStrings( args:String* ) = {
  var i : Int = 0;
  for( arg <- args ){
    println("Arg value[" + i + "] = " + arg );
    i = i + 1;
  }
}
  //指定默认值函数的参数
  def addInt( a:Int=5, b:Int=7 ) : Int = {
    var sum:Int = 0
    sum = a + b

    return sum
  }
  //高阶函数的定义,采取其他函数参数，或它的结果是一个功能的函数
  def apply(f: Int => String, v: Int) = f(v)

  def layout[A](x: A) = "[" + x.toString() + "]"

  //Scala匿名函数
  var inc = (x:Int) => x+1
  //变量inc现在可以使用以通常的方式的函数：
  var x = inc(7)-1
  //用多个参数定义的函数如下：
  var mul = (x: Int, y: Int) => x*y
 // println(mul(3, 4))
  //无参数定义函数如下：
  var userDir = () => { System.getProperty("user.dir") }
 // println( userDir )

  //Scala柯里函数
//柯里转换函数接受多个参数成一条链的函数，每次取一个参数
def strcat(s1: String)(s2: String) = {
  s1 + s2
}
  //二种定义柯里函数
  //def strcat(s1: String) = (s2: String) => s1 + s2


  //Scala嵌套函数
  //阶乘计算器,在这里使用调用第二个函数
  def factorial(i: Int): Int = {
    def fact(i: Int, accumulator: Int): Int = {
      if (i <= 1)
        accumulator
      else
        fact(i - 1, i * accumulator)
    }
    fact(i, 1)
  }

  //Scala部分应用函数
  //
  def log(date: Date, message: String)  = {
    println(date + "----" + message)
  }

  //Scala的Option[T]是容器对于给定的类型的零个或一个元件
  def show(x: Option[String]) = x match {
    case Some(s) => s
    case None => "?"
  }

  //
}
