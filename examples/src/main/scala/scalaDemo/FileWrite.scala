package scalaDemo

import java.io.{File, FileWriter}
import java.util.Random

import com.google.common.base.Charsets.UTF_8
import com.google.common.io.Files
import org.apache.spark.util.Utils

object FileWrite {
  def main(args: Array[String]) {


    val outFile = File.createTempFile("test-load-spark-properties", "test")
    Files.write("spark.test.fileNameLoadA true\n" +
      "spark.test.fileNameLoadB 1\n", outFile, UTF_8)


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