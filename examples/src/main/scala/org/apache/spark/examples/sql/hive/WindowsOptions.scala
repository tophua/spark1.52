package org.apache.spark.examples.sql.hive


import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random
/**
  * Created by liush on 17-7-3.
  */
case class Order(
                  name: String,
                  clas: Int,
                  s: Int)
//select student_name, class, score, sum(score) over(partition by class order by score desc) 累计 from temp_b;
case class ClassSchool(
                        student_name: String,
                        class_name: String,
                        score: Int)


case class City(
                 id: Int,
                 name: String,
                 code: Int,
                 prov_id: String
               )

case class t(
              bill_month: String,
              area_code: Int,
              net_type: String,
              local_fare: Double
            )
object WindowsOptions {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HiveFromSpark").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    import hiveContext.sql
    def _sqlContext: SQLContext = hiveContext


    val sqlContext = _sqlContext
    /*  sql("SHOW TABLES").toString
      sql("SELECT * FROM src").toString
      sql("drop TABLE  src")*/
    sql("USE db_shanxi_test")
    sqlContext.udf.register("random0", (min:Int,max:Int) => {  val random =  new Random()
      val s: Int = random.nextInt(max) % (max - min + 1) + min
      s
    })

    println("sql自定义函数:"+sql("SELECT random0(1,10) ").head().getInt(0) )
    windowsFuncitonTest(hiveContext)
  }

  def windowsFuncitonTest(hiveContext:HiveContext): Unit ={
    import hiveContext.sql

    val test = Seq(
      ClassSchool("张三","A",90),
      ClassSchool("李四","A",95),
      ClassSchool("王五","A",85),
      ClassSchool("芳芳","B",92),
      ClassSchool("明明","B",78),
      ClassSchool("亮亮","B",78),
      ClassSchool("晶晶","B",75)
    )
    import hiveContext.implicits._
    //集合自动隐式转换
    test.toDF.registerTempTable("temp_b")
    //分班级按成绩排名次排序如下：
    // 函数dense_rank()是连续排序，有两个第二名时仍然跟着第三名。
    // 函数rank()是跳跃排序，有两个第二名时接下来就是第四名（同样是在各个分组内）
    /**
    +------------+----------+-----+-------+
        |student_name|class_name|score|mingchi|
        +------------+----------+-----+-------+
        |李四          |A         |95   |1      |
        |张三          |A         |90   |2      |
        |王五          |A         |85   |3      |
        |芳芳          |B         |92   |1      |
        |明明          |B         |78   |2      |
        |亮亮          |B         |78   |2      |
        |晶晶          |B         |75   |3      |
        +------------+----------+-----+-------+
      */
    sql(
      """
        |select student_name, class_name, score, dense_rank() over(partition by class_name order by score desc) mingchi from temp_b
      """.stripMargin).show(false)

    /**
      * +------------+----------+-----+-------+
      |student_name|class_name|score|mingchi|
      +------------+----------+-----+-------+
      |李四          |A         |95   |1      |
      |张三          |A         |90   |2      |
      |王五          |A         |85   |3      |
      |芳芳          |B         |92   |1      |
      |明明          |B         |78   |2      |
      |亮亮          |B         |78   |2      |
      |晶晶          |B         |75   |4      |
      +------------+----------+-----+-------+
      */
    sql(
      """
        |select student_name, class_name, score, rank() over(partition by class_name order by score desc) mingchi from temp_b
      """.stripMargin).show(false)

    println("rank:分班级按成绩排名次排序如下：")
    sql(
      """
        |select student_name, class_name, score, rank() over(partition by class_name order by score desc) mingchi from temp_b
      """.stripMargin).show(false)
    println("班级成绩累计（\"连续\"求和）结果如下：")
    /**
      * +------------+----------+-----+---+
        |student_name|class_name|score|sum|
        +------------+----------+-----+---+
        |李四          |A         |95   |95 |
        |张三          |A         |90   |185|
        |王五          |A         |85   |270|
        |芳芳          |B         |92   |92 |
        |明明          |B         |78   |248|
        |亮亮          |B         |78   |248|
        |晶晶          |B         |75   |323|
        +------------+----------+-----+---+
      */
    sql(
      """
        |  select student_name, class_name, score, sum(score) over(partition by class_name order by score desc) sum from temp_b
      """.stripMargin).show(false)
    println("已班级分组成绩求和,不带排序：")
    /**
    +------------+----------+-----+--------+
      |student_name|class_name|score|classsum|
      +------------+----------+-----+--------+
      |张三          |A         |90   |270     |
      |李四          |A         |95   |270     |
      |王五          |A         |85   |270     |
      |芳芳          |B         |92   |323     |
      |明明          |B         |78   |323     |
      |亮亮          |B         |78   |323     |
      |晶晶          |B         |75   |323     |
      +------------+----------+-----+--------+
      */
    sql(
      """
        |   select student_name, class_name, score, sum(score) over(partition by class_name) classsum from temp_b
      """.stripMargin).show(false)
    //总结:带排序的分组连续求和,不带排序分组求和
  }

}
