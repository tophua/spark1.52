package scalaDemo

/**
  * Created by liush on 17-7-21.
  */

abstract class UndoableAction(val description: String) {
  def undo(): Unit
  def redo(): Unit
}

object DoNothingAction extends UndoableAction("Do nothing") {
  override def undo() {}
  override def redo() {}
}
class Employee(val name: String)
class Manager {
  import collection.mutable._
  val subordinates = new ArrayBuffer[Employee]
  def description = "A manager with " + subordinates.length + " subordinates"
}


class PersonFive {
  var name = ""
}

class EmployeeTwo extends PersonFive {
  var salary = 0.0
  def description = "An employee with name " + name + " and salary " + salary
}


class PersonSix {
  var name = ""
  override def toString = getClass.getName + "[name=" + name + "]"
}

class EmployeeThree extends PersonSix {
  var salary = 0.0
  override def toString = super.toString + "[salary=" + salary + "]"
}


object AbstractClass {

  def main(args: Array[String]):Unit= {
    val wilma = new Manager
    val employees = collection.mutable.HashSet(
      // import collection.mutable._ doesn't extend until here
      new Employee("Fred"), new Employee("Barney"))
    //可变数组和HashSet操作
    wilma.subordinates ++= employees
    println(wilma + ": " + wilma.description)
    val actions = Map("open" -> DoNothingAction, "save" -> DoNothingAction)
    println("===="+actions("open").description)
    println(actions("open") == actions("save"))

    val fred = new EmployeeTwo
    fred.name = "Fred"
    fred.salary = 50000
    println(fred.description)

    val fredb = new EmployeeThree
    fredb.name = "Fred"
    fredb.salary = 50000
    println(fredb)
  }
}
