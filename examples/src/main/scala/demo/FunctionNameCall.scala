package demo

/**
  * Created by liush on 17-7-14.
  * Scala函数按名称调用
  * 通常情况下，函数的参数是传值参数;即，参数的值在它被传递给函数之前被确定。但是，如果我们需要编写一个接收参数不希望马上计算，直到调用函数内的表达式。
  * 对于这种情况，Scala提供按名称参数调用函数。
  * 按名称调用机制传递一个代码块给被调用者并且每次被调用方传接入参数，代码块被执行，值被计算。
  */
object FunctionNameCall {
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
}
