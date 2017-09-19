package scalaDemo

/**
  * Created by liush on 17-8-15.
  * 偏函数是个特质其的类型为PartialFunction[A,B],其中接收一个类型为A的参数,返回一个类型为B的结果
  */
object PartialFunctionTest  extends  App{

  //def isDefinedAt(x: A): Boolean //作用是判断传入来的参数是否在这个偏函数所处理的范围内
  //定义一个普通的除法函数：
  val divide = (x : Int) => 100/x
  //当我们将0作为参数传入时会报错,一般的解决办法就是使用try/catch来捕捉异常或者对参数进行判断看其是否等于0
 // divide(0)
  //但是在Scala的偏函数中这些都已经封装好了,如下：将divide定义成一个偏函数()
  val dividePart = new PartialFunction[Int,Int] {
    def isDefinedAt(x: Int): Boolean = x != 0 //判断x是否等于0，当x = 0时抛出异常
    def apply(x: Int): Int = 100/x
  }
  //但是,当偏函数与case语句结合起来就能使代码更简洁,如下：
  val divide1 : PartialFunction[Int,Int] = {
    case d : Int if d != 0 => 100/d //功能和上面的代码一样，这就是偏函数的强大之处，方便，简洁！！
  }
  //其实这代码就是对上面那段代码的封装，这里同样调用isDefinedAt方法
  divide1.isDefinedAt(0)
  //res1: Boolean = false
  divide1.isDefinedAt(10)
  //res2: Boolean = true　

  //再举个与case结合使用的例子：
  val rs : PartialFunction[Int , String] = {
    case 1 => "One"
    case 2 => "Two"
    case _ => "Other"
  }
  rs(1)
  rs(2)

}
