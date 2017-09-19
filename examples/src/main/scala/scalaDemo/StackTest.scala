package scalaDemo

/**
  * Created by liush on 17-8-28.
  */
object StackTest extends  App{
  import scala.collection.mutable.Stack
  val stack = new Stack[Int]
  stack.push(1)//在最顶层加入数据
  stack.push(2)//在最顶层加入数据
 stack.top //返回并移除最顶层的数据
  //res8: Int = 2
 stack.pop //返回最顶层数据的值,但不移除它
 // res10: Int = 2
  println(stack)
//res11: scala.collection.mutable.Stack[Int] = Stack(1)
}
