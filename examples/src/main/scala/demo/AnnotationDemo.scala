package demo


import scala.annotation.target._
import scala.reflect.BeanProperty
import javax.persistence.Entity
import javax.persistence.Id
import javax.validation.constraints.NotNull
import scala.annotation.target.beanGetter
/**
  * Created by liush on 17-7-21.
  */
// Annotation for class
@Entity class Credentials(@NotNull @BeanProperty var username: String) {
  // @NotNull is only applied to the constructor parameter, not to
  // the getters/setters
  def check(@NotNull password: String) {}
  // @NotNull is applied to the method parameter
  @BeanProperty @deprecated("Use check instead", "1.5") var pwd = ""
  // @deprecated is applied to the Scala and bean getters/setters
  @(Id @beanGetter) @BeanProperty var id = 0
  // @Id is only applied to the bean getter
}

object AnnotationDemo extends App {

  val creds = new Credentials("Fred")
  creds.pwd = "secret" // Deprecation warning for Scala setter
  println(creds.getPwd()) // Deprecation warning for bean getter
}
