package demo

/**
  * Created by liush on 17-7-21.
  */

import scala.beans.BeanProperty

class Person {
  @BeanProperty var name : String = _
}
object BeanDemo {
  def main(args: Array[String]):Unit= {
    val fred = new Person
    fred.setName("Fred")
    println("==="+fred.getName)
  }
}
