package scalaDemo

/**
  * Created by liush on 17-7-14.
  */
object RelationalOperatorTest {
  def main(args: Array[String]) {
    var a = 10;
    var b = 20;
    //==检查两个操作数的值是否相等，如果是的话那么条件为真
    println("a == b = " + (a == b) );
    //!=检查两个操作数的值是否相等，如果值不相等，则条件变为真。
    println("a != b = " + (a != b) );
    println("a > b = " + (a > b) );
    println("a < b = " + (a < b) );
    println("b >= a = " + (b >= a) );
    println("b <= a = " + (b <= a) );


    //Scala语言支持以下赋值运算符

     a = 10
     b = 20
    var c = 0

    c = a + b
    println("c = a + b  = " + c)
  //加法和赋值运算符，它增加了右操作数左操作数和分配结果左操作数
    //C += A 相当于 C = C + A
    c += a
    println("c += a  = " + c)
  //减和赋值运算符 C -= A 相当于 C = C - A
    c -= a
    println("c -= a = " + c)
  //乘法和赋值运算符 C *= A 相当于 C = C * A
    c *= a
    println("c *= a = " + c)

    a = 10
    c = 15
    //除法和赋值运算符 C /= A 相当于 C = C / A
    c /= a
    println("c /= a  = " + c)

    a = 10
    c = 15
    c %= a
    println("c %= a  = " + c)
    //左移位并赋值运算符,C <<= 2 等同于 C = C << 2
    c <<= 2
    println("c <<= 2  = " + c)
  //向右移位并赋值运算符,C >>= 2 等同于 C = C >> 2
    c >>= 2
    println("c >>= 2  = " + c)

    c >>= 2
    println("c >>= a  = " + c)
    //按位与赋值运算符
    c &= a
    println("c &= 2  = " + c)
  //按位异或并赋值运算符
    c ^= a
    println("c ^= a  = " + c)
    // 按位或并赋值运算符
    c |= a
    println("c |= a  = " + c)
  }
}
