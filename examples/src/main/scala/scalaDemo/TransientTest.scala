package scalaDemo

import java.beans.Transient
import java.io._

/**
  * Created by liush on 17-7-20.
  */

class User extends Serializable{
   var name: String = _
   @transient var age :String = _
}
object TransientTest {
  //@transient注解一般用于序列化的时候，标识某个字段不用被序列化
  //由于age被标记为@transient，在反序列化的时候,就获取不到原始值了所以被赋值为默认值
  class Hippie(val name: String, @transient val age: Int) extends Serializable
  def main(args: Array[String]): Unit = {
    var user = new User()
    user.name="Alexia"
    user.age="12"
    val p1 = new Hippie("zml", 34)



    println("read before Serializable: ")
/*    println("name: " + p1.name)
    println("age: " + p1.age)*/

    println("username: " + user.name)
    println("password: " + user.age)
    val os = new ObjectOutputStream(new FileOutputStream("/home/liush/s3/user.txt"))
      os.writeObject(user) // 将User对象写进文件
   // os.writeObject(p1)
    os.close()
      os.flush()
      os.close()
    val is = new ObjectInputStream(new FileInputStream("/home/liush/s3/user.txt"));
   // Map[String, Integer] deserializedMap = (Map < String, Integer >)
   //   var usera= is.readObject().asInstanceOf[User] // 从流中读取User的数据
   /*var p2= is.readObject().asInstanceOf[Hippie] // 从流中读取User的数据
      is.close()
      println("\nread after Serializable: ")
      println("name: " + p2.name)
      println("age: " + p2.age)*/
   var usera= is.readObject().asInstanceOf[User] // 从流中读取User的数据
    is.close()
    println("\nread after Serializable: ")
    println("name: " + usera.name)
    println("age: " + usera.age)


  }

}