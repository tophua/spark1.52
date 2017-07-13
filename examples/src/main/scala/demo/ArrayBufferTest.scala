package demo

import scala.collection.mutable.ArrayBuffer

/**
  * Created by liush on 17-7-13.
  * 变长数组（即数组缓冲）：java中有ArrayList和scala中的ArrayBuffer等效；
  * 但是ArrayBuffer更加强大，通过下面的事列来熟悉ArrayBuffer：
  */
object ArrayBufferTest {
  def main(args: Array[String]):Unit= {
    val arrbuff1 = ArrayBuffer[Int]()
    val arrBuff2 = ArrayBuffer(1,3,4,-1,-4)
    arrbuff1 += 23    //用+=在尾端添加元素
    arrbuff1 += (2,3,4,32) //同时在尾端添加多个元素
    arrbuff1 ++= arrBuff2 //可以用 ++=操作符追加任何集合
    println("arrbuff1 ++= arrBuff2:"+arrbuff1.mkString(","))
    arrbuff1 ++= Array(2,43,88,66)
    println("++= Array:"+arrbuff1.mkString(","))
    arrbuff1.trimEnd(2) //移除最后的2个元素
    println("trimEnd(2):"+arrbuff1.mkString(","))
    arrbuff1.remove(2)  //移除arr(2+1)索引元素
    println("remove(2):"+arrbuff1.mkString(","))
    arrbuff1.remove(2,4) //从第三个元素开始移除4个元素
    val arr = arrbuff1.toArray //将数组缓冲转换为Array
    val arrbuff2 = arrbuff1.toBuffer //将Array转换为数组缓冲
   // 3、遍历数组和数组缓冲：在java中数组和数组列表/向量上语法有些不同。scala则更加统一，通常情况，我们可以用相同的代码处理这两种数据结构，for(…) yield 循环创建一个类型和原集合类型相同的新集合。for循环中还可以带守卫：在for中用if来实现。
    println("arrBuff1:"+arrbuff1.mkString(","))
    println("arrBuff2:"+arrBuff2.mkString(","))
    val tset= for(i <- 0 until arrBuff2.length)
      yield arrbuff1(i) * 2 //将得到ArrayBuffer(2,6,4,-2,-4)
    println(tset.mkString(","))
   val test1= for(i <- 0 until (arrbuff1.length,2))
      yield arrbuff1(i) * 2 //将得到ArrayBuffer(12,-4)

    for(elem <-arrBuff2)
      println("==for="+elem) //如果不需要使用下标，用这种方式最简单了

    for(i <- arrbuff1 if i > 2)
      println("=="+i)//打印出arrbuff1中为整数的值
    arrbuff1.filter( _ > 0).map{ 2 * _} //生成arrbuff1中的正数的两倍的新集合
    arrbuff1.filter {_ > 0} map {2 * _} //另一种写法
   // 4、常用算法：scala有很多便捷内建函数，如
   println("sum:"+ arrbuff1.sum) //对arrbuff1元素求和
    Array("asd","sdf","ss").max //求最大元素
    //println(arrbuff1.sorted(_<_ ))  //将arrbuff1元素从小到大排序
   // println(arrbuff1.sorted(_> _))  //从大到小排序
    println("==="+util.Sorting.quickSort(Array("asd","sdf","ss"))) //针对数组排序，单不能对数组缓冲排序
    val arrb = Array(1,23,4,2,45)
    println(arrb.mkString(",")) //指定分割符
    println(arrb.mkString("(",",",")")) //指定前缀、分隔符、后缀*/

  }

}
