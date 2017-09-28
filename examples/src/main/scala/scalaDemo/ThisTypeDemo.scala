package scalaDemo

/**
  * 从此可以看出在方法返回类型上使用this.type就可以写出像链条一样的代码，不断地点出来:)
  */
object ThisTypeDemo extends App {
  //食物点出来
  val food = new Food
  food.setName("rice").setAge(2)
  println("Food : " + food)
  //大米点出来
  val rice = new Rice
  //这是没问题的因为返回的是Food
  rice.setName("guangxidami").setAge(3)
  //这样也没问题，setgrow()返回大米这个对象，可以调用父类的setName，setAge方法
  rice.setgrow().setName("beijingdami").setAge(1)
  //这样在没修改返回类型为this.type之前是有问题的，因为setName，setAge返回的是食物这个类，
  //食物没有setgrow()这个方法
  rice.setName("zhejiangdami").setAge(4).setgrow()
  println("Rice : " + rice)

  var sumPredictions = 111
  //===断言、检查
  assert(sumPredictions == 0.0,
    "MyLogisticRegression predicted something other than 0, even though all weights are 0!")

}

/**
  * 定义食物这个类,里面有食物的名字还有年龄
  */
class Food {
  private var name: String = _
  private var age: Int = _

  def setName(getName: String): this.type = {
    this.name = getName
    this
  }

  def setAge(getAge: Int): this.type = {
    this.age = getAge
    this
  }

  /*
  def setName(getName: String) = {
    this.name = getName
    this
  }
  def setAge(getAge: Int)= {
    this.age = getAge
    this
  }
   */

  override def toString: String = "name = " + name + "||=|| age = " + age
}

/**
  * 定义一个大米类继承食物，里面方法返回的this是大米这个对象
  */
class Rice extends Food {

  def setgrow() = {
    println(" I am growing!! Don't eat me :(")
    this
  }
}


