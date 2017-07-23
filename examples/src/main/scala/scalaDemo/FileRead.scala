package scalaDemo

/**
  * Created by liush on 17-7-14.
  *  读取文件
  */
object FileRead {
  import scala.io.Source
  def main(args: Array[String]):Unit= {

      for (line <- Source.fromFile("/home/liush/s3/S3_2016001.txt").getLines)
        println(line.length + " " + line)

  }
}
