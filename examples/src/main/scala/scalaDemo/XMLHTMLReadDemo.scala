package scalaDemo

import scala.xml.Text

/**
  * Created by liush on 17-7-21.
  */
object XMLHTMLReadDemo extends App{
  import scala.xml._

  val items = Array("Fred", "Wilma")
  val lst = <ul><li>{items(0)}</li><li>{items(1)}</li></ul>

  val containsAtoms = <p>{42}{"Fred"}{Text("Wilma")}</p>
  for (n <- containsAtoms.child) {
    println(n.getClass.getName + ": " + n)
  }

  val lsta = <ul>{for (i <- items) yield <li>{i}</li>}</ul>

    <h1>The Natural Numbers {{1, 2, 3, ...}}</h1>





}
