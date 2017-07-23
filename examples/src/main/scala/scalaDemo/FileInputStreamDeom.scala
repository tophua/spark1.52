package scalaDemo

import java.io._
import java.nio.file.{Files, Paths}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by liush on 17-7-21.
  */

object FileInputStreamDeom extends App {


  val source = Source.fromFile("/home/liush/s3/S3_2016001.txt", "UTF-8")
  val lineIterator = source.getLines

  for (l <- lineIterator)
    println(if (l.length <= 13) l else l.substring(0, 10) + "...")

  source.close()

  // Caution: The sources below aren't closed.
  //警告：下面的来源没有关闭
  val lines = Source.fromFile("/home/liush/s3/S3_2016001.txt", "UTF-8").getLines.toArray

//repl-session.zip
  val file = new File("/home/liush/s3/S3_2016001.txt")
  val in = new FileInputStream(file)
  val bytes = new Array[Byte](file.length.toInt)
  in.read(bytes)
  in.close()

  print(f"Zip files starts with ${bytes(0)}%c${bytes(1)}%c, the initials of their inventor.%n")

  val source1 = Source.fromURL("http://horstmann.com", "UTF-8")
  println(source1.mkString.length + " bytes")
  val source2 = Source.fromString("Hello, World!")
  println(source2.mkString.length + " bytes")
  // Reads from the given string—useful for debugging
  println("What is your name?")
  //val source3 = Source.stdin
  // Reads from standard input
 // println("Hello, " + source3.getLines.next)


/*  val out = new PrintWriter("numbers.txt")
  for (i <- 1 to 10) out.writeUTF("%6d %10.6f\n".format(i, 1.0 / i))
  out.close()*/



  class Person(val name: String) extends Serializable {
    val friends = new ArrayBuffer[Person]
    // OK—ArrayBuffer is serializable
    def description = name + " with friends " +
      friends.map(_.name).mkString(", ")
  }

  val fred = new Person("Fred")
  val wilma = new Person("Wilma")
  val barney = new Person("Barney")
  fred.friends += wilma
  fred.friends += barney
  wilma.friends += barney
  barney.friends += fred

  val outobj = new ObjectOutputStream(new FileOutputStream("test.obj"))
  outobj.writeObject(fred)
  outobj.close()
  val inobj = new ObjectInputStream(new FileInputStream("test.obj"))
  val savedFred = inobj.readObject().asInstanceOf[Person]
  inobj.close()

  println("ObjectOut="+savedFred.description)
  savedFred.friends.map(_.description)

}