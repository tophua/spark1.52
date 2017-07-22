package org.apache.spark.examples.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.functions.{concat_ws, countDistinct}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by liush on 17-7-21.
  */
object SparkSQLGroup extends  App{
  val sparkConf = new SparkConf().setMaster("local").setAppName("RDDRelation")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
  val hiveContext = new HiveContext(sc)
  import hiveContext.implicits._
  import hiveContext.sql
  //使用Seq造数据，四列数据
  val df = sc.parallelize(Seq(
    (0,"p1",30.9,"2017-03-04"),
    (0,"u",22.1,"2017-03-05"),
    (1,"r",19.6,"2017-03-04"),
    (2,"cat40",20.7,"2017-03-05"),
    (3,"cat187",27.9,"2017-03-04"),
    (4,"cat183",11.3,"2017-03-06"),
    (5,"cat8",35.6,"2017-03-08"))

  ).toDF("id", "name", "price","dt")//转化df的四列数据s
  //创建表明为pro
  df.show(false)
  df.registerTempTable("pro")//注册对象
  /**
    a       b1
    a       b2
    a       b2
    c       d1
    c       d1
    d       d2
    =======================
    a       ["b1","b2","b2"]
    c       ["d1","d1"]
    d       ["d2"]
    **/
    val db = sc.parallelize(Seq(
    ("a","b1"),
    ("a","b2"),
    ("a","b2"),
    ("c","d1"),
    ("c","d1"),
    ("d","d2"))
  ).toDF("col1", "col2")
  db.registerTempTable("tmp_liush")
  db.show(false)
  /**
  +----+------------+
  |col1|_c1         |
  +----+------------+
  |a   |[b1, b2, b2]|
  |c   |[d1, d1]    |
  |d   |[d2]        |
  +----+------------+ */
  //行转列,collect_list函数返回的类型是array< ？ >类型，？表示该列的类型,用collect_set函数进行去重
 sql( "select col1, collect_list(col2) from tmp_liush group by col1").show(false)
   val dfp = Seq((1, "a b c"), (2, "a b"), (3, "a")).toDF("number", "letters")
  /**
    +------+-------+
    |number|letters|
    +------+-------+
    |1     |a b c  |
    |2     |a b    |
    |3     |a      |
    +------+-------+
    */
  dfp.show(false)
   //explode函数实现行转列
  val df2 =
      dfp.explode('letters) {
        //使用case Row形式分隔,注意Tuple1元组形式
        case Row(letters: String) =>
          //println(letters)

          letters.split(" ").map(Tuple1(_)
        ).toSeq
      }
  /**
    +------+-------+---+
    |number|letters|_1 |
    +------+-------+---+
    |1     |a b c  |a  |
    |1     |a b c  |b  |
    |1     |a b c  |c  |
    |2     |a b    |a  |
    |2     |a b    |b  |
    |3     |a      |a  |
    +------+-------+---+
    */
  df2.show(false)

  // agg将每个分区里面的元素进行聚合,返回dataframe类型,同数学计算求值
  /**
    * 计算非重复结果的数目 countDistinct
    +------+----------------------+
    |letter|COUNT(DISTINCT number)|
    +------+----------------------+
    |     a|                     3|
    |     b|                     2|
    |     c|                     1|
    +------+----------------------+
    */
  df2.select('_1 as 'letter, 'number).groupBy('letter).agg(countDistinct('number)).show()
}
