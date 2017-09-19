package scalaDemo

/**
 * 
 */

object MyEnum extends Enumeration {
  //定义三个字段,然后用Value调用将它们初始化,每次调用Value都返回内部类的新实例,该内部类也叫做Value,
  //或者你也可以向Value方法传入ID/名称/两个参数都传
  val Red, Yellow, Green = Value
}
object Hi {
  def main(args: Array[String]) {
    //可以通过如下语句直接引入枚举值
    //import MyEnum._
    //记住枚举的类型是MyEnum.Value而不是MyEnum
    //type MyEnum = Value //此句话用来设置类型别名,这样的话,枚举的类型就变成了MyEnum.MyEnum
    //枚举值的ID可通过id方法返回,名称通过toString方法返回,对MyEnum.values的调用输出所有枚举值的集合
    //可以通过枚举的ID或名称来进行查找定位,以下两句输出内容一样
    println(MyEnum.values.toString())     
    println(MyEnum(0))
    println(MyEnum.withName("Red"))
  }
}
