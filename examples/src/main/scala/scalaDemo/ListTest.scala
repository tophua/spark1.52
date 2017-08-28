package scalaDemo

/**
  * Created by liush on 17-7-13.
  */
object ListTest {
  def main(args: Array[String]):Unit= {
    //创建列表
    val days = List("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")
    //创建空列表
    val l = Nil
    //用字符串创建列表
    val l2 = "Hello" :: "Hi" :: "Hah" :: "WOW" :: "WOOW" :: Nil
    //用“:::”叠加创建新列表
    val wow = l ::: List("WOOOW", "WOOOOW")
    //通过索引获取列表值
   println("通过索引获取列表值:"+l2(3))

    //返回去掉l头两个元素的新列表
    println("返回去掉l头两个元素的新列表:"+ l2.drop(2))
    //返回去掉l后两个元素的新列表
    println("返回去掉l后两个元素的新列表"+l2.dropRight(2))
    //判断l是否存在某个元素
    println("判断l是否存在某个元素:"+ l2.exists(s => s == "Hah"))

    //取出第一个元素
    println("取出第一个元素"+l2.head)
    //取出最后一个元素
    println("取出最后一个元素"+l2.last)
    //剔除最后一个元素，生成新列表
    println("剔除最后一个元素，生成新列表:"+l2.init)
    //剔除第一个元素，生成新列表
    println("剔除第一个元素，生成新列表:"+l2.tail)
    //判断列表是否为空
    println("判断列表是否为空:"+l2.isEmpty)
    //获得列表长度
    println("获得列表长度:"+l2.length)

    //生成用逗号隔开的字符串
    println("生成用逗号隔开的字符串:"+l2.mkString(", "))
    //反序生成新列表
    println("反序生成新列表:"+l2.reverse)

   //获取值长度为3的元素数目
    println("获取值长度为3的元素数目:"+l2.count(s => s.length == 3))

   //滤出长度为3的元素
    println("滤出长度为3的元素:"+l2.filter(s => s.length == 3))
   //判断所有元素是否以“H”打头
    println("判断所有元素是否以“H”打头:"+l2.forall(s => s.startsWith("H")))
   //判断所有元素是否以“H”结尾
    println("判断所有元素是否以“H”结尾:"+l2.forall(s => s.endsWith("W")))
   //打印每个元素
    println("判断所有元素是否以“H”结尾:"+l2.foreach(s => print(s + ' ')))
   //修改每个元素，再反转每个元素形成新列表
    println("修改每个元素，再反转每个元素形成新列表:"+l2.map(s => {val s1 = s + " - 01"; s1.reverse}))
   //按字母递增排序
    println("按字母递增排序:"+l2.sortWith(_.compareTo(_) < 0) )  /**/

    println("list(1):"+List(1))
    //indices返回所有有效索引值
    List("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday").indices.map(index =>println(index))
  }

}
