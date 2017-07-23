package scalaDemo

/**
  * Created by liush on 17-7-21.
  */

import scala.beans.BeanProperty
import scala.collection.mutable.ArrayBuffer

class Person {
  @BeanProperty var name : String = _
}

class PersonBean {
  private var name = ""
  private var age = 0

  def this(name: String) { // An auxiliary constructor
    this() // Calls primary constructor
    this.name = name
  }
  def this(name: String, age: Int) { // Another auxiliary constructor
    this(name) // Calls previous auxiliary constructor
    this.age = age
  }

  def description = name + " is " + age + " years old"
}
/**
  * 构造函数
  * @param name
  * @param age
  */
class PersonTwo(val name: String = "", val age: Int = 0) {
  println("Just constructed another person")
  def description = name + " is " + age + " years old"
}

/**
  *  构造函数私有
  * @param name
  * @param age
  */
class PersonThree(val name: String, private var age: Int) {
  def description = name + " is " + age + " years old"
  def birthday() { age += 1 }
}
class Network(val name: String) { outer =>
  class Member(val name: String) {
    val contacts = new ArrayBuffer[Member]
    def description = name + " inside " + outer.name
  }

  private val members = new ArrayBuffer[Member]

  def join(name: String) = {
    val m = new Member(name)
    members += m
    m
  }
}
object BeanDemo {
  def main(args: Array[String]):Unit= {
    val fred = new Person
    fred.setName("Fred")
    println("==="+fred.getName)
    val p1 = new PersonBean // Primary constructor
    val p2 = new PersonBean("Fred") // First auxiliary constructor
    val p3 = new PersonBean("Fred", 42) // Second auxiliary constructor

    p1.description
    p2.description
    p3.description

    println("==p1:=="+p1.description+"==p2:=="+p2.description+"==p3:=="+p3.description)


    val p4 = new PersonTwo
    val p5 = new PersonTwo("Fred")
    val p6 = new PersonTwo("Fred", 42)
    p4.description
    p5.description
    p6.description
    println("==p4:=="+p4.description+"==p5:=="+p5.description+"==p6:=="+p6.description)


    val p7 = new PersonThree("Fred", 42)
    p7.name
   // p7.age // Error--it's private
    p7.birthday()
    p7.description
    println("==p7name:=="+p7.name+"==birthday:=="+ p7.birthday()+"==description:=="+  p7.description)


    val chatter = new Network("Chatter")
    val myFace = new Network("MyFace")

    val fredb = chatter.join("Fred")
    println(fredb.description);
    val barney = myFace.join("Barney")
    println(barney.description);
  }
}
