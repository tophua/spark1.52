package scalaDemo

/**
  * Created by liush on 17-7-23.
  */


  object optionDemo extends App {
    println("basics")
    val x: Option[String] = None  //创建未初始化的字符串变量
    //println(x.get)//访问未初始化的变量导致抛出异常
    println(x.getOrElse("default"))//使用默认值的方式访问
    val y = Some("now initialized")//用字符串初始化
    println(y.get)//访问未初始化成功
    println(y.getOrElse("default"))//没用使用默认值

    println("\noption factory")
    val a = Option(null)//null转化成Option类型,如果输入null,创建None对象,如果输入是初始化了的值,创建一个Some对象
    val b = Option("hello")
    println(a.getOrElse("default"), b.get)

    println("\nCREATE AN OBJECT OR RETURN A DEFAULT")
    /**
      * Option重要体性是可以被当作集合看待,可以使用标准map,flatmap,foreach等方法,
      * getTemporaryDirectory接受Option类型的参数,返回指向我们使用的临时文件目录的File对象,
      * 首先对option应用map放法,在参数有值的情况下创建一个Filed对象,然后我们使用File方法检查option
      * 里是否符合断言要求,如果不符合就转化为None,最后我们检查Option里是否有值,如果没有还回默认的文件路径
      * @param tmpArg
      * @return
      */
    def getTemporaryDirectory(tmpArg: Option[String]): java.io.File = {
      tmpArg.map(name => new java.io.File(name)).filter(_.isDirectory).getOrElse(new java.io.File(System.getProperty("java.io.tmpdir")))
    }
    println(getTemporaryDirectory(None))
    println(getTemporaryDirectory(Option("hello")))
    println(getTemporaryDirectory(Option("c:\\temp")))

    println("\nEXECUTE BLOCK OF CODE IF VARIABLE IS INITIALIZED")
    //如果变量已初始化执行代码块,可以通过foreach方法遍历Option所有值,因为Option只能零个或一个值,
    //所以代码要么执行,要么不执行
    def printname(username: Option[String]) {
      for (uname <- username) {
        println("User: " + uname)
      }
    }
    printname(Option(null))
    printname(Option("glorysdj"))

    def compareTwoNum(no1: Option[Int], no2: Option[Int]) = {
      for (n1 <- no1; n2 <- no2) {
        if (n1 > n2) println("bigger")
        else if (n1 == n2) println("equals")
        else println("smaller")
      }
    }
    compareTwoNum(Option(1), None)
    compareTwoNum(None, Option(2))
    compareTwoNum(Option(3), Option(2))

  }

