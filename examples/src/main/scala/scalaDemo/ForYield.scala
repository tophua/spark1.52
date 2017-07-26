package scalaDemo

/**
  * Created by liush on 17-7-26.
  */
object ForYield {

  def main(args: Array[String]): Unit = {
    //for表达式可以包含1个或多个「生成器」
    for (i <- 1 to 3; j <- 1 to 3)
      println (i  + "==="+j)
    /**
      1===1
      1===2
      1===3
      2===1
      2===2
      2===3
      3===1
      3===2
      3===3
      */
    //每个生成器都可以带一个守卫，以if开头的Boolean表达式：
    //注意在if之前并没有分号,
    for (i <- 1 to 3; j <- 1 to 3  if i != j){
      // 将打印 12 13 21 23 31 32
      println ((10 * i + j) + " ")
    }
    //如果f o r 循环的循环体以yield开始，则该循环会构造出一个集合，每次迭代生成集中的一个值：
    // 生成 Vector(1, 2, 0, 1, 2, 0, 1, 2, 0, 1)
    val t=for (i <- 1 to 10)  yield i % 3
    println(t)
    // 将生成 "HIeflmlmop"
   val p= for (c <- "Hello"; i <- 0 to 1) yield (c + i).toChar
    println(p)
  }

}
