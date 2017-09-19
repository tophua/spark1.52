package org.apache.spark.examples.sql
import scala.collection.mutable.{ ListBuffer, Queue }

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

case class Person(name: String, age: Int)

/**
 * 简单的Spark SQL应用例子 
 */
object SparkSqlExample {
  def main(args: Array[String]) {
    //System.getenv()和System.getProperties()的区别
    //System.getenv() 返回系统环境变量值 设置系统环境变量：当前登录用户主目录下的".bashrc"文件中可以设置系统环境变量
    //System.getProperties() 返回Java进程变量值 通过命令行参数的"-D"选项
    sys.env.foreach(println _)
    //获得环境变量信息(JAVA_HOME,D:\jdk\jdk17_64)
    val conf = sys.env.get("SPARK_AUDIT_MASTER") match {
      case Some(master) => new SparkConf().setAppName("Simple Sql App").setMaster(master)
      case None         => new SparkConf().setAppName("Simple Sql App").setMaster("local")
    }
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
    import sqlContext._
    //生成RDD,转换成DataFrame对象
    val people = sc.makeRDD(1 to 100, 10).map(x => Person(s"Name$x", x)).toDF()
    people.registerTempTable("people")//注册对象 
    //返回DataFrame,查询年龄大于等于13小于等于19
    val teenagers = sql("SELECT name,age FROM people WHERE age >= 13 AND age <= 19")
    //返回 teenagerNames: Array[String],获得SQL语句name,age字段值
    val teenagerNames = teenagers.map(t => "Name: " + t(0)+ " age:"+t(1)).collect()
    teenagerNames.foreach(println)

    def test(f: => Boolean, failureMsg: String) = {
      if (!f) {
        println(failureMsg)
        System.exit(-1)
      }
    }
    //异常数量的选定元素
    test(teenagerNames.size == 7, "Unexpected number of selected elements: " + teenagerNames)
    println("Test succeeded")
    sc.stop()
  }

}
