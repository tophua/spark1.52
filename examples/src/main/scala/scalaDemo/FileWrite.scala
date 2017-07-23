package scalaDemo

import java.io.{File, FileWriter}
import java.util.Random

object FileWrite {
  def main(args: Array[String]) {
    val writer = new FileWriter(new File("D:\\eclipse44_64\\workspace\\spark1.5\\examples\\sample_age_data.txt"), false)
    val rand = new Random()
    for (i <- 1 to 10000) {
      writer.write(i + " " + rand.nextInt(100))
      writer.write(System.getProperty("line.separator"))
    }
    writer.flush()
    writer.close()
  }
}