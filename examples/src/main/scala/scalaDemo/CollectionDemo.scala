package scalaDemo

/**
  * Created by liush on 17-7-21.
  */
object CollectionDemo extends  App {

  val coll = Array(1, 7, 2, 9) // some Iterable
  val iter = coll.iterator
  while (iter.hasNext)
    println(iter.next())

  import java.awt.Color

  Iterable(0xFF, 0xFF00, 0xFF0000)
  Set(Color.RED, Color.GREEN, Color.BLUE)
  Map(Color.RED -> 0xFF0000, Color.GREEN -> 0xFF00, Color.BLUE -> 0xFF)

  import scala.collection._

  SortedSet("Hello", "World").foreach(println _)




  val mutableMap = new collection.mutable.HashMap[String, Int]

  val map: collection.Map[String, Int] = mutableMap

  mutableMap.put("Wilma", 17)
  //map.put("Fred", 29) // Error

  import scala.collection.mutable

  val immutableMap = Map("Hello" -> 42)

  //val mutableMap = new mutable.HashMap[String, Int]

  def digits(n: Int): Set[Int] =
    if (n < 0) { digits(-n)
    } else if (n < 10) { Set(n)
    } else digits(n / 10) + (n % 10)

  digits(1729)


  val vec = (1 to 1000000) map (_ % 100)
  // map transforms a Range into a Vector
  val lst = vec.toList
  //泛型匿名函数
  def time[T](block: => T) = {
    val start = System.nanoTime
    val result = block
    val elapsed = System.nanoTime - start
    println(elapsed + " nanoseconds")
    result
  }

  time(vec(500000))

  time(lst(500000))


  def sum(lst: List[Int]): Int =
  //List判度
  //Nil是一个空的List,::向队列的头部追加数据,创造新的列表
    if (lst == Nil) 0 else lst.head + sum(lst.tail)
  println("sum:"+sum(List(9, 4, 2))+"==sum:"+List(9, 4, 2).sum)

  import scala.collection.mutable.ArrayBuffer

  val numbers = ArrayBuffer(1, 2, 3)
  numbers += 5

  var numberSet = Set(1, 2, 3)
  numberSet += 5 // Sets numberSet to the immutable set numberSet + 5
  numberSet
  var numberVector = Vector(1, 2, 3)
  numberVector :+= 5 // += does not work since vectors don't have a + operator
  numberVector

  Set(1, 2, 3) - 2

  numbers ++ Vector(1, 2, 7, 9)

  numbers -- Vector(1, 2, 7, 9)



  val words = "Mary had a little lamb".split(" ")

  words.reverse

  words.sorted
  words.sortWith(_.length < _.length)
  words.sortBy(_.length)

  words.permutations.toArray
  words.combinations(3).toArray

  "-3+4".collect { case '+' => 1 ; case '-' => -1 }

  val freq = scala.collection.mutable.Map[Char, Int]()
  for (c <- "Mississippi") freq(c) = freq.getOrElse(c, 0) + 1


  println("Scala".zipWithIndex)

  println("Scala".zipWithIndex.max)

  println("Scala".zipWithIndex.max._2)




}
