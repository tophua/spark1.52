package scalaDemo

/**
  * create by liush on 2017-12-21
  */
object MapOption {

  def main(args: Array[String]):Unit= {
    val mapOption=Map()
    val colors1 = Map("red" -> "#FF0000",
      "azure" -> "#F0FFFF",
      "peru" -> "#CD853F")
    val colors2 = Map("blue" -> "#0033FF",
      "yellow" -> "#FFFF00",
      "red" -> "#FF0000")
    println("====>"+mapOption.isEmpty)
    mapOption.++(colors2)
    println("====>"+mapOption.isEmpty)
  }
}
