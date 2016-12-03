/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import java.sql.{Timestamp, Date}
import java.text.SimpleDateFormat

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.unsafe.types.CalendarInterval
/**
 * 日期函数测试
 */
class DateFunctionsSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("function current_date") {//当前日期函数
    val df1 = Seq((1, 2), (3, 1)).toDF("a", "b")
    //H2数据库日期函数
    val d0 = DateTimeUtils.millisToDays(System.currentTimeMillis())
   // println(d0)
    val d1 = DateTimeUtils.fromJavaDate(df1.select(current_date()).collect().head.getDate(0))
    //2016-12-02
    println(df1.select(current_date()).collect().head.getDate(0))
    /**
     *+----------+
      |       _c0|
      +----------+
      |2016-12-02|
      +----------+
     */
    sql("""SELECT CURRENT_DATE()""").show()
    val d2 = DateTimeUtils.fromJavaDate(
      sql("""SELECT CURRENT_DATE()""").collect().head.getDate(0))
    val d3 = DateTimeUtils.millisToDays(System.currentTimeMillis())
    assert(d0 <= d1 && d1 <= d2 && d2 <= d3 && d3 - d0 <= 1)
  }

  // This is a bad test. SPARK-9196 will fix it and re-enable it.
  //这是一个坏的测试,将修复它并重新启用它
  ignore("function current_timestamp") {//当前时间戳函数
    val df1 = Seq((1, 2), (3, 1)).toDF("a", "b")
    checkAnswer(df1.select(countDistinct(current_timestamp())), Row(1))
    // Execution in one query should return the same value
    //在一个查询中的执行应该返回相同的值
    checkAnswer(sql("""SELECT CURRENT_TIMESTAMP() = CURRENT_TIMESTAMP()"""),
      Row(true))
    assert(math.abs(sql("""SELECT CURRENT_TIMESTAMP()""").collect().head.getTimestamp(
      0).getTime - System.currentTimeMillis()) < 5000)
  }

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val sdfDate = new SimpleDateFormat("yyyy-MM-dd")
  val d = new Date(sdf.parse("2015-04-08 13:10:15").getTime)
  val ts = new Timestamp(sdf.parse("2013-04-08 13:10:15").getTime)

  test("timestamp comparison with date strings") {//日期时间戳比较字符串
    val df = Seq(
      (1, Timestamp.valueOf("2015-01-01 00:00:00")),
      (2, Timestamp.valueOf("2014-01-01 00:00:00"))).toDF("i", "t")

    checkAnswer(
      //时间的比较查询
      df.select("t").filter($"t" <= "2014-06-01"),//日期小于查询
      Row(Timestamp.valueOf("2014-01-01 00:00:00")) :: Nil)


    checkAnswer(
      df.select("t").filter($"t" >= "2014-06-01"),//日期大于查询
      Row(Timestamp.valueOf("2015-01-01 00:00:00")) :: Nil)
  }

  test("date comparison with date strings") {//日期字符串的日期比较
    val df = Seq(
      (1, Date.valueOf("2015-01-01")),
      (2, Date.valueOf("2014-01-01"))).toDF("i", "t")

    checkAnswer(
      df.select("t").filter($"t" <= "2014-06-01"),
      Row(Date.valueOf("2014-01-01")) :: Nil)


    checkAnswer(//查询条件年,也能查询出来
      df.select("t").filter($"t" >= "2015"),
      Row(Date.valueOf("2015-01-01")) :: Nil)
  }

  test("date format") {//日期格式化
    val df = Seq((d, sdf.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
        //使用函数日期格式化为年
      df.select(date_format($"a", "y"), date_format($"b", "y"), date_format($"c", "y")),
      Row("2015", "2015", "2013"))

    checkAnswer(
      df.selectExpr("date_format(a, 'y')", "date_format(b, 'y')", "date_format(c, 'y')"),
      Row("2015", "2015", "2013"))
  }

  test("year") {//年
    val df = Seq((d, sdfDate.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
        //使用year函数提取年度
      df.select(year($"a"), year($"b"), year($"c")),
      Row(2015, 2015, 2013))

    checkAnswer(
      //使用year函数提取年度
      df.selectExpr("year(a)", "year(b)", "year(c)"),
      Row(2015, 2015, 2013))
  }

  test("quarter") {//季度
    val ts = new Timestamp(sdf.parse("2013-11-08 13:10:15").getTime)

    val df = Seq((d, sdfDate.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      //使用quarter函数提取季度
      df.select(quarter($"a"), quarter($"b"), quarter($"c")),
      Row(2, 2, 4))

    checkAnswer(
      df.selectExpr("quarter(a)", "quarter(b)", "quarter(c)"),
      Row(2, 2, 4))
  }

  test("month") {//月
    //提取月
    val df = Seq((d, sdfDate.format(d), ts)).toDF("a", "b", "c")
    //使用month函数提取月
    checkAnswer(
      df.select(month($"a"), month($"b"), month($"c")),
      Row(4, 4, 4))

    checkAnswer(
      df.selectExpr("month(a)", "month(b)", "month(c)"),
      Row(4, 4, 4))
  }

  test("dayofmonth") {//这个月的第几天
    val df = Seq((d, sdfDate.format(d), ts)).toDF("a", "b", "c")
     //使用dayofmonth函数提取月的第几天
    checkAnswer(
      df.select(dayofmonth($"a"), dayofmonth($"b"), dayofmonth($"c")),
      Row(8, 8, 8))

    checkAnswer(
      df.selectExpr("day(a)", "day(b)", "dayofmonth(c)"),
      Row(8, 8, 8))
  }

  test("dayofyear") {//这个年的第几天
    val df = Seq((d, sdfDate.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      //使用dayofyear函数提取年的第几天
      df.select(dayofyear($"a"), dayofyear($"b"), dayofyear($"c")),
      Row(98, 98, 98))

    checkAnswer(
      df.selectExpr("dayofyear(a)", "dayofyear(b)", "dayofyear(c)"),
      Row(98, 98, 98))
  }

  test("hour") {//小时
    val df = Seq((d, sdf.format(d), ts)).toDF("a", "b", "c")    
    //使用hour函数提取小时
    checkAnswer(
      df.select(hour($"a"), hour($"b"), hour($"c")),
      Row(0, 13, 13))

    checkAnswer(
      df.selectExpr("hour(a)", "hour(b)", "hour(c)"),
      Row(0, 13, 13))
  }

  test("minute") {//分钟
    val df = Seq((d, sdf.format(d), ts)).toDF("a", "b", "c")
    /**
     *+----------+-------------------+--------------------+
      |         a|                  b|                   c|
      +----------+-------------------+--------------------+
      |2015-04-08|2015-04-08 13:10:15|2013-04-08 13:10:...|
      +----------+-------------------+--------------------+
     */
    df.show()
    checkAnswer(
       //使用minute函数提取分钟
      df.select(minute($"a"), minute($"b"), minute($"c")),
      Row(0, 10, 10))

    checkAnswer(
      df.selectExpr("minute(a)", "minute(b)", "minute(c)"),
      Row(0, 10, 10))
  }

  test("second") {//秒
    val df = Seq((d, sdf.format(d), ts)).toDF("a", "b", "c")
     /**
     *+----------+-------------------+--------------------+
      |         a|                  b|                   c|
      +----------+-------------------+--------------------+
      |2015-04-08|2015-04-08 13:10:15|2013-04-08 13:10:...|
      +----------+-------------------+--------------------+
     */
    
    checkAnswer(
      //使用second函数提取秒
      df.select(second($"a"), second($"b"), second($"c")),
      Row(0, 15, 15))

    checkAnswer(
      df.selectExpr("second(a)", "second(b)", "second(c)"),
      Row(0, 15, 15))
  }

  test("weekofyear") {//这年的第几周
    val df = Seq((d, sdfDate.format(d), ts)).toDF("a", "b", "c")
     /**
     *+----------+-------------------+--------------------+
      |         a|                  b|                   c|
      +----------+-------------------+--------------------+
      |2015-04-08|2015-04-08 13:10:15|2013-04-08 13:10:...|
      +----------+-------------------+--------------------+
     */
    checkAnswer(
      //使用weekofyear函数提取一年第几周
      df.select(weekofyear($"a"), weekofyear($"b"), weekofyear($"c")),
      Row(15, 15, 15))

    checkAnswer(
      df.selectExpr("weekofyear(a)", "weekofyear(b)", "weekofyear(c)"),
      Row(15, 15, 15))
  }

  test("function date_add") {//日期增加函数
    val st1 = "2015-06-01 12:34:56"
    val st2 = "2015-06-02 12:34:56"
    val t1 = Timestamp.valueOf(st1)
    val t2 = Timestamp.valueOf(st2)
    val s1 = "2015-06-01"
    val s2 = "2015-06-02"
    val d1 = Date.valueOf(s1)
    val d2 = Date.valueOf(s2)
    val df = Seq((t1, d1, s1, st1), (t2, d2, s2, st2)).toDF("t", "d", "s", "ss")
    /**
     *+--------------------+----------+----------+-------------------+
      |                   t|         d|         s|                 ss|
      +--------------------+----------+----------+-------------------+
      |2015-06-01 12:34:...|2015-06-01|2015-06-01|2015-06-01 12:34:56|
      |2015-06-02 12:34:...|2015-06-02|2015-06-02|2015-06-02 12:34:56|
      +--------------------+----------+----------+-------------------+
     */
     df.show()
    checkAnswer(
      //日期添加一天       
      df.select(date_add(col("d"), 1)),
      Seq(Row(Date.valueOf("2015-06-02")), Row(Date.valueOf("2015-06-03"))))
    checkAnswer(
      //日期添加三天      
      df.select(date_add(col("t"), 3)),
      Seq(Row(Date.valueOf("2015-06-04")), Row(Date.valueOf("2015-06-05"))))
    checkAnswer(
       //日期添加五天      
      df.select(date_add(col("s"), 5)),
      Seq(Row(Date.valueOf("2015-06-06")), Row(Date.valueOf("2015-06-07"))))
    checkAnswer(
       //日期添加七天      
      df.select(date_add(col("ss"), 7)),
      Seq(Row(Date.valueOf("2015-06-08")), Row(Date.valueOf("2015-06-09"))))
      //使用DATE_ADD函数添加1天,如果字段为null,添加不能添加
    checkAnswer(df.selectExpr("DATE_ADD(null, 1)"), Seq(Row(null), Row(null)))
    checkAnswer(
      df.selectExpr("""DATE_ADD(d, 1)"""),
      Seq(Row(Date.valueOf("2015-06-02")), Row(Date.valueOf("2015-06-03"))))
  }

  test("function date_sub") {//日期减去指定的时间间隔
    val st1 = "2015-06-01 12:34:56"
    val st2 = "2015-06-02 12:34:56"
    val t1 = Timestamp.valueOf(st1)
    val t2 = Timestamp.valueOf(st2)
    val s1 = "2015-06-01"
    val s2 = "2015-06-02"
    val d1 = Date.valueOf(s1)
    val d2 = Date.valueOf(s2)
    val df = Seq((t1, d1, s1, st1), (t2, d2, s2, st2)).toDF("t", "d", "s", "ss")
    /**
     *+--------------------+----------+----------+-------------------+
      |                   t|         d|         s|                 ss|
      +--------------------+----------+----------+-------------------+
      |2015-06-01 12:34:...|2015-06-01|2015-06-01|2015-06-01 12:34:56|
      |2015-06-02 12:34:...|2015-06-02|2015-06-02|2015-06-02 12:34:56|
      +--------------------+----------+----------+-------------------+
     */
    df.show()
    checkAnswer(
      //日期减1天函数
      df.select(date_sub(col("d"), 1)),
      Seq(Row(Date.valueOf("2015-05-31")), Row(Date.valueOf("2015-06-01"))))
    checkAnswer(
       //时间日期减1天函数
      df.select(date_sub(col("t"), 1)),
      Seq(Row(Date.valueOf("2015-05-31")), Row(Date.valueOf("2015-06-01"))))
    checkAnswer(
       //时间日期减1天函数
      df.select(date_sub(col("s"), 1)),
      Seq(Row(Date.valueOf("2015-05-31")), Row(Date.valueOf("2015-06-01"))))
    checkAnswer(
      df.select(date_sub(col("ss"), 1)),
      Seq(Row(Date.valueOf("2015-05-31")), Row(Date.valueOf("2015-06-01"))))
    checkAnswer(
       //使用date_sub函数减1天,如果字段为null,添加不能减
      df.select(date_sub(lit(null), 1)).limit(1), Row(null))

    checkAnswer(df.selectExpr("""DATE_SUB(d, null)"""), Seq(Row(null), Row(null)))
    checkAnswer(
      df.selectExpr("""DATE_SUB(d, 1)"""),
      Seq(Row(Date.valueOf("2015-05-31")), Row(Date.valueOf("2015-06-01"))))
  }

  test("time_add") {//时间添加
    val t1 = Timestamp.valueOf("2015-07-31 23:59:59")
    val t2 = Timestamp.valueOf("2015-12-31 00:00:00")
    val d1 = Date.valueOf("2015-07-31")
    val d2 = Date.valueOf("2015-12-31")
    val i = new CalendarInterval(2, 2000000L)
    val df = Seq((1, t1, d1), (3, t2, d2)).toDF("n", "t", "d")
    /**
     *+---+--------------------+----------+
      |  n|                   t|         d|
      +---+--------------------+----------+
      |  1|2015-07-31 23:59:...|2015-07-31|
      |  3|2015-12-31 00:00:...|2015-12-31|
      +---+--------------------+----------+
     */
    df.show()
    checkAnswer(
       //$引用变量
      df.selectExpr(s"d + $i"),
      Seq(Row(Date.valueOf("2015-09-30")), Row(Date.valueOf("2016-02-29"))))
      
    checkAnswer(
        //$引用变量
      df.selectExpr(s"t + $i"),
      Seq(Row(Timestamp.valueOf("2015-10-01 00:00:01")),
        Row(Timestamp.valueOf("2016-02-29 00:00:02"))))
  }

  test("time_sub") {//时间减去指定的时间间隔
    val t1 = Timestamp.valueOf("2015-10-01 00:00:01")
    val t2 = Timestamp.valueOf("2016-02-29 00:00:02")
    val d1 = Date.valueOf("2015-09-30")
    val d2 = Date.valueOf("2016-02-29")
    val i = new CalendarInterval(2, 2000000L)
    val df = Seq((1, t1, d1), (3, t2, d2)).toDF("n", "t", "d")
    checkAnswer(
      df.selectExpr(s"d - $i"),
      Seq(Row(Date.valueOf("2015-07-30")), Row(Date.valueOf("2015-12-30"))))
    checkAnswer(
      df.selectExpr(s"t - $i"),
      Seq(Row(Timestamp.valueOf("2015-07-31 23:59:59")),
        Row(Timestamp.valueOf("2015-12-31 00:00:00"))))
  }

  test("function add_months") {//将一个日期上加上一指定的月份数
    val d1 = Date.valueOf("2015-08-31")
    val d2 = Date.valueOf("2015-02-28")
    val df = Seq((1, d1), (2, d2)).toDF("n", "d")
    checkAnswer(
       //将一个日期上加上一指定的月份数,注意2月添加一个月是3月31天
      df.select(add_months(col("d"), 1)),
      Seq(Row(Date.valueOf("2015-09-30")), Row(Date.valueOf("2015-03-31"))))
    checkAnswer(
       //add_months如果参数为-1数则减去月数
      df.selectExpr("add_months(d, -1)"),
      Seq(Row(Date.valueOf("2015-07-31")), Row(Date.valueOf("2015-01-31"))))
  }

  test("function months_between") {//返回两个日期之间的月份数
    val d1 = Date.valueOf("2015-07-31")
    val d2 = Date.valueOf("2015-02-16")
    val t1 = Timestamp.valueOf("2014-09-30 23:30:00")
    val t2 = Timestamp.valueOf("2015-09-16 12:00:00")
    val s1 = "2014-09-15 11:30:00"
    val s2 = "2015-10-01 00:00:00"
    val df = Seq((t1, d1, s1), (t2, d2, s2)).toDF("t", "d", "s")
    /**
     *+--------------------+----------+-------------------+
      |                   t|         d|                  s|
      +--------------------+----------+-------------------+
      |2014-09-30 23:30:...|2015-07-31|2014-09-15 11:30:00|
      |2015-09-16 12:00:...|2015-02-16|2015-10-01 00:00:00|
      +--------------------+----------+-------------------+ */
    df.show()
    //返回两个日期之间的月份数,注意列名引用col
    checkAnswer(df.select(months_between(col("t"), col("d"))), Seq(Row(-10.0), Row(7.0)))
    //内置函数的使用
    checkAnswer(df.selectExpr("months_between(t, s)"), Seq(Row(0.5), Row(-0.5)))
  }

  test("function last_day") {//返回指定日期对应月份的最后一天
    val df1 = Seq((1, "2015-07-23"), (2, "2015-07-24")).toDF("i", "d")
    val df2 = Seq((1, "2015-07-23 00:11:22"), (2, "2015-07-24 11:22:33")).toDF("i", "t")
    checkAnswer(
      df1.select(last_day(col("d"))),
      Seq(Row(Date.valueOf("2015-07-31")), Row(Date.valueOf("2015-07-31"))))
    checkAnswer(
      df2.select(last_day(col("t"))),
      Seq(Row(Date.valueOf("2015-07-31")), Row(Date.valueOf("2015-07-31"))))
  }

  test("function next_day") {//返回输入日期开始
    val df1 = Seq(("mon", "2015-07-23"), ("tuesday", "2015-07-20")).toDF("dow", "d")
    val df2 = Seq(("th", "2015-07-23 00:11:22"), ("xx", "2015-07-24 11:22:33")).toDF("dow", "t")
    checkAnswer(
      df1.select(next_day(col("d"), "MONDAY")),
      Seq(Row(Date.valueOf("2015-07-27")), Row(Date.valueOf("2015-07-27"))))
    checkAnswer(
      df2.select(next_day(col("t"), "th")),
      Seq(Row(Date.valueOf("2015-07-30")), Row(Date.valueOf("2015-07-30"))))
  }

  test("function to_date") {//转换日期函数
    val d1 = Date.valueOf("2015-07-22")
    val d2 = Date.valueOf("2015-07-01")
    val t1 = Timestamp.valueOf("2015-07-22 10:00:00")
    val t2 = Timestamp.valueOf("2014-12-31 23:59:59")
    val s1 = "2015-07-22 10:00:00"
    val s2 = "2014-12-31"
    val df = Seq((d1, t1, s1), (d2, t2, s2)).toDF("d", "t", "s")

    checkAnswer(
      df.select(to_date(col("t"))),
      Seq(Row(Date.valueOf("2015-07-22")), Row(Date.valueOf("2014-12-31"))))
    checkAnswer(
      df.select(to_date(col("d"))),
      Seq(Row(Date.valueOf("2015-07-22")), Row(Date.valueOf("2015-07-01"))))
    checkAnswer(
      df.select(to_date(col("s"))),
      Seq(Row(Date.valueOf("2015-07-22")), Row(Date.valueOf("2014-12-31"))))

    checkAnswer(
      df.selectExpr("to_date(t)"),
      Seq(Row(Date.valueOf("2015-07-22")), Row(Date.valueOf("2014-12-31"))))
    checkAnswer(
      df.selectExpr("to_date(d)"),
      Seq(Row(Date.valueOf("2015-07-22")), Row(Date.valueOf("2015-07-01"))))
    checkAnswer(
      df.selectExpr("to_date(s)"),
      Seq(Row(Date.valueOf("2015-07-22")), Row(Date.valueOf("2014-12-31"))))
  }

  test("function trunc") {//截取日期函数
    val df = Seq(
      (1, Timestamp.valueOf("2015-07-22 10:00:00")),
      (2, Timestamp.valueOf("2014-12-31 00:00:00"))).toDF("i", "t")

    checkAnswer(
      df.select(trunc(col("t"), "YY")),
      Seq(Row(Date.valueOf("2015-01-01")), Row(Date.valueOf("2014-01-01"))))

    checkAnswer(
      df.selectExpr("trunc(t, 'Month')"),
      Seq(Row(Date.valueOf("2015-07-01")), Row(Date.valueOf("2014-12-01"))))
  }

  test("from_unixtime") {//以"YYYY-MM-DD"格式来显示的字符
    val sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val fmt2 = "yyyy-MM-dd HH:mm:ss.SSS"
    val sdf2 = new SimpleDateFormat(fmt2)
    val fmt3 = "yy-MM-dd HH-mm-ss"
    val sdf3 = new SimpleDateFormat(fmt3)
    val df = Seq((1000, "yyyy-MM-dd HH:mm:ss.SSS"), (-1000, "yy-MM-dd HH-mm-ss")).toDF("a", "b")
    checkAnswer(
      df.select(from_unixtime(col("a"))),
      Seq(Row(sdf1.format(new Timestamp(1000000))), Row(sdf1.format(new Timestamp(-1000000)))))
    checkAnswer(
      df.select(from_unixtime(col("a"), fmt2)),
      Seq(Row(sdf2.format(new Timestamp(1000000))), Row(sdf2.format(new Timestamp(-1000000)))))
    checkAnswer(
      df.select(from_unixtime(col("a"), fmt3)),
      Seq(Row(sdf3.format(new Timestamp(1000000))), Row(sdf3.format(new Timestamp(-1000000)))))
    checkAnswer(
      df.selectExpr("from_unixtime(a)"),
      Seq(Row(sdf1.format(new Timestamp(1000000))), Row(sdf1.format(new Timestamp(-1000000)))))
    checkAnswer(
      df.selectExpr(s"from_unixtime(a, '$fmt2')"),
      Seq(Row(sdf2.format(new Timestamp(1000000))), Row(sdf2.format(new Timestamp(-1000000)))))
    checkAnswer(
      df.selectExpr(s"from_unixtime(a, '$fmt3')"),
      Seq(Row(sdf3.format(new Timestamp(1000000))), Row(sdf3.format(new Timestamp(-1000000)))))
  }

  test("unix_timestamp") {//unix时间戳
    val date1 = Date.valueOf("2015-07-24")
    val date2 = Date.valueOf("2015-07-25")
    val ts1 = Timestamp.valueOf("2015-07-24 10:00:00.3")
    val ts2 = Timestamp.valueOf("2015-07-25 02:02:02.2")
    val s1 = "2015/07/24 10:00:00.5"
    val s2 = "2015/07/25 02:02:02.6"
    val ss1 = "2015-07-24 10:00:00"
    val ss2 = "2015-07-25 02:02:02"
    val fmt = "yyyy/MM/dd HH:mm:ss.S"
    val df = Seq((date1, ts1, s1, ss1), (date2, ts2, s2, ss2)).toDF("d", "ts", "s", "ss")
    checkAnswer(df.select(unix_timestamp(col("ts"))), Seq(
      Row(ts1.getTime / 1000L), Row(ts2.getTime / 1000L)))
    checkAnswer(df.select(unix_timestamp(col("ss"))), Seq(
      Row(ts1.getTime / 1000L), Row(ts2.getTime / 1000L)))
    checkAnswer(df.select(unix_timestamp(col("d"), fmt)), Seq(
      Row(date1.getTime / 1000L), Row(date2.getTime / 1000L)))
    checkAnswer(df.select(unix_timestamp(col("s"), fmt)), Seq(
      Row(ts1.getTime / 1000L), Row(ts2.getTime / 1000L)))
    checkAnswer(df.selectExpr("unix_timestamp(ts)"), Seq(
      Row(ts1.getTime / 1000L), Row(ts2.getTime / 1000L)))
    checkAnswer(df.selectExpr("unix_timestamp(ss)"), Seq(
      Row(ts1.getTime / 1000L), Row(ts2.getTime / 1000L)))
    checkAnswer(df.selectExpr(s"unix_timestamp(d, '$fmt')"), Seq(
      Row(date1.getTime / 1000L), Row(date2.getTime / 1000L)))
    checkAnswer(df.selectExpr(s"unix_timestamp(s, '$fmt')"), Seq(
      Row(ts1.getTime / 1000L), Row(ts2.getTime / 1000L)))
  }

  test("datediff") {//表示两个指定日期间的时间间隔数目
    val df = Seq(
      (Date.valueOf("2015-07-24"), Timestamp.valueOf("2015-07-24 01:00:00"),
        "2015-07-23", "2015-07-23 03:00:00"),
      (Date.valueOf("2015-07-25"), Timestamp.valueOf("2015-07-25 02:00:00"),
        "2015-07-24", "2015-07-24 04:00:00")
    ).toDF("a", "b", "c", "d")
    checkAnswer(df.select(datediff(col("a"), col("b"))), Seq(Row(0), Row(0)))
    checkAnswer(df.select(datediff(col("a"), col("c"))), Seq(Row(1), Row(1)))
    checkAnswer(df.select(datediff(col("d"), col("b"))), Seq(Row(-1), Row(-1)))
    checkAnswer(df.selectExpr("datediff(a, d)"), Seq(Row(1), Row(1)))
  }

  test("from_utc_timestamp") {//来自UTC时间戳
    val df = Seq(
      (Timestamp.valueOf("2015-07-24 00:00:00"), "2015-07-24 00:00:00"),
      (Timestamp.valueOf("2015-07-25 00:00:00"), "2015-07-25 00:00:00")
    ).toDF("a", "b")
    checkAnswer(
      df.select(from_utc_timestamp(col("a"), "PST")),
      Seq(
        Row(Timestamp.valueOf("2015-07-23 17:00:00")),
        Row(Timestamp.valueOf("2015-07-24 17:00:00"))))
    checkAnswer(
      df.select(from_utc_timestamp(col("b"), "PST")),
      Seq(
        Row(Timestamp.valueOf("2015-07-23 17:00:00")),
        Row(Timestamp.valueOf("2015-07-24 17:00:00"))))
  }

  test("to_utc_timestamp") {//转换UTC时间戳
    val df = Seq(
      (Timestamp.valueOf("2015-07-24 00:00:00"), "2015-07-24 00:00:00"),
      (Timestamp.valueOf("2015-07-25 00:00:00"), "2015-07-25 00:00:00")
    ).toDF("a", "b")
    checkAnswer(
      df.select(to_utc_timestamp(col("a"), "PST")),
      Seq(
        Row(Timestamp.valueOf("2015-07-24 07:00:00")),
        Row(Timestamp.valueOf("2015-07-25 07:00:00"))))
    checkAnswer(
      df.select(to_utc_timestamp(col("b"), "PST")),
      Seq(
        Row(Timestamp.valueOf("2015-07-24 07:00:00")),
        Row(Timestamp.valueOf("2015-07-25 07:00:00"))))
  }

}
