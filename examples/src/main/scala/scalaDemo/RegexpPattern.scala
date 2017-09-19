package scalaDemo

/**
  * Created by liush on 17-7-21.
  */
object RegexpPattern extends App {
  val numPattern = "[0-9]+".r

  val wsnumwsPattern = """\s+[0-9]+\s+""".r

  for (matchString <- numPattern.findAllIn("99 bottles, 98 bottles"))
    println(matchString)

  val matches = wsnumwsPattern.findFirstIn("99 bottles, 98 bottles")

  val firstMatch = wsnumwsPattern.findFirstIn("99 bottles, 98 bottles")


  val anchoredPattern = "^[0-9]+$".r
  val str = " 123"
  //None被声明为一个对象,而不是一个类,在没有值的时候,使用None,如果有值可以引用,就使用Some来包含这个值,都是Option的子类
  if (anchoredPattern.findFirstIn(str) == None) println("Not a number")
  if (str.matches("[0-9]+")) println("A number")

  numPattern.replaceFirstIn("99 bottles, 98 bottles", "XX")

  numPattern.replaceAllIn("99 bottles, 98 bottles", "XX")

  numPattern.replaceSomeIn("99 bottles, 98 bottles",
    //None被声明为一个对象,而不是一个类,在没有值的时候,使用None,如果有值可以引用,就使用Some来包含这个值,都是Option的子类
    m => if (m.matched.toInt % 2 == 0) Some("XX") else None)

  val varPattern = """\$[0-9]+""".r
  def format(message: String, vars: String*) =
    varPattern.replaceSomeIn(message, m => vars.lift(
      m.matched.tail.toInt))
  format("At $1, there was $2 on $0.",
    "planet 7", "12:30 pm", "a disturbance of the force")


  val numitemPattern = "([0-9]+) ([a-z]+)".r

  for (m <- numitemPattern.findAllMatchIn("99 bottles, 98 bottles"))
    println(m.group(1))

  val numitemPattern(num, item) = "99 bottles"

  for (numitemPattern(num, item) <- numitemPattern.findAllIn("99 bottles, 98 bottles"))
    println(item + ": " + num)


}
