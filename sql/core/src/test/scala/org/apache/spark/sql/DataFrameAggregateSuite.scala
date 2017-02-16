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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.DecimalType

//数据集聚合测试
class DataFrameAggregateSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("groupBy") {//分组
    /**
     *+---+------+
      |  a|sum(b)|
      +---+------+
      |  1|     3|
      |  2|     3|
      |  3|     3|
      +---+------+
     */
    testData2.groupBy("a").agg(sum($"b")).show()
    checkAnswer(
      testData2.groupBy("a").agg(sum($"b")),//df.agg() 求聚合用的相关函数
      Seq(Row(1, 3), Row(2, 3), Row(3, 3))
    )
    checkAnswer(
      testData2.groupBy("a").agg(sum($"b").as("totB")).agg(sum('totB)),
      Row(9)
    )
    checkAnswer(
      testData2.groupBy("a").agg(count("*")),//df.agg() 求聚合用的相关函数
      Row(1, 2) :: Row(2, 2) :: Row(3, 2) :: Nil
    )
    checkAnswer(
      testData2.groupBy("a").agg(Map("*" -> "count")),//df.agg() 求聚合用的相关函数
      Row(1, 2) :: Row(2, 2) :: Row(3, 2) :: Nil
    )
    checkAnswer(
      testData2.groupBy("a").agg(Map("b" -> "sum")),
      Row(1, 3) :: Row(2, 3) :: Row(3, 3) :: Nil
    )

    val df1 = Seq(("a", 1, 0, "b"), ("b", 2, 4, "c"), ("a", 2, 3, "d"))
      .toDF("key", "value1", "value2", "rest")

    checkAnswer(
      df1.groupBy("key").min(),
      df1.groupBy("key").min("value1", "value2").collect()
    )
    checkAnswer(
      df1.groupBy("key").min("value2"),
      Seq(Row("a", 0), Row("b", 4))
    )
  }

  test("spark.sql.retainGroupColumns config") {//保留组列配置
    checkAnswer(
      testData2.groupBy("a").agg(sum($"b")),//df.agg() 求聚合用的相关函数
      Seq(Row(1, 3), Row(2, 3), Row(3, 3))
    )

    ctx.conf.setConf(SQLConf.DATAFRAME_RETAIN_GROUP_COLUMNS, false)
    checkAnswer(
      testData2.groupBy("a").agg(sum($"b")),//df.agg() 求聚合用的相关函数
      Seq(Row(3), Row(3), Row(3))
    )
    ctx.conf.setConf(SQLConf.DATAFRAME_RETAIN_GROUP_COLUMNS, true)
  }

  test("agg without groups") {//无分组
    checkAnswer(
      testData2.agg(sum('b)),//df.agg() 求聚合用的相关函数
      Row(9)
    )
  }

  test("average") {//平均值
    checkAnswer(
      testData2.agg(avg('a)),//df.agg() 求聚合用的相关函数
      Row(2.0))

    // Also check mean
      //同时检查的中间
    checkAnswer(
      testData2.agg(mean('a)),//df.agg() 求聚合用的相关函数
      Row(2.0))

    checkAnswer(//df.agg() 求聚合用的相关函数
      testData2.agg(avg('a), sumDistinct('a)), // non-partial
      Row(2.0, 6.0) :: Nil)

    checkAnswer(
      decimalData.agg(avg('a)),//df.agg() 求聚合用的相关函数
      Row(new java.math.BigDecimal(2.0)))
    checkAnswer(
      decimalData.agg(avg('a), sumDistinct('a)), // non-partial
      Row(new java.math.BigDecimal(2.0), new java.math.BigDecimal(6)) :: Nil)

    checkAnswer(
      decimalData.agg(avg('a cast DecimalType(10, 2))),//df.agg() 求聚合用的相关函数
      Row(new java.math.BigDecimal(2.0)))
    // non-partial
    checkAnswer(//df.agg() 求聚合用的相关函数
      decimalData.agg(avg('a cast DecimalType(10, 2)), sumDistinct('a cast DecimalType(10, 2))),
      Row(new java.math.BigDecimal(2.0), new java.math.BigDecimal(6)) :: Nil)
  }

  test("null average") {//空平均
    checkAnswer(
      testData3.agg(avg('b)),
      Row(2.0))

    checkAnswer(//df.agg() 求聚合用的相关函数
      testData3.agg(avg('b), countDistinct('b)),
      Row(2.0, 1))

    checkAnswer(//df.agg() 求聚合用的相关函数
      testData3.agg(avg('b), sumDistinct('b)), // non-partial
      Row(2.0, 2.0))
  }

  test("zero average") {//零平均
    val emptyTableData = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(
      emptyTableData.agg(avg('a)),//df.agg() 求聚合用的相关函数
      Row(null))

    checkAnswer(//df.agg() 求聚合用的相关函数
      emptyTableData.agg(avg('a), sumDistinct('b)), // non-partial
      Row(null, null))
  }

  test("count") {//计数
    assert(testData2.count() === testData2.map(_ => 1).count())

    checkAnswer(//df.agg() 求聚合用的相关函数
      testData2.agg(count('a), sumDistinct('a)), // non-partial
      Row(6, 6.0))
  }

  test("null count") {//空计算
    checkAnswer(
      testData3.groupBy('a).agg(count('b)),//df.agg() 求聚合用的相关函数
      Seq(Row(1, 0), Row(2, 1))
    )

    checkAnswer(
      testData3.groupBy('a).agg(count('a + 'b)),//df.agg() 求聚合用的相关函数
      Seq(Row(1, 0), Row(2, 1))
    )

    checkAnswer(//df.agg() 求聚合用的相关函数
      testData3.agg(count('a), count('b), count(lit(1)), countDistinct('a), countDistinct('b)),
      Row(2, 1, 2, 2, 1)
    )

    checkAnswer(//df.agg() 求聚合用的相关函数
      testData3.agg(count('b), countDistinct('b), sumDistinct('b)), // non-partial
      Row(1, 1, 2)
    )
  }

  test("zero count") {//零计数
    val emptyTableData = Seq.empty[(Int, Int)].toDF("a", "b")
    assert(emptyTableData.count() === 0)

    checkAnswer(
      emptyTableData.agg(count('a), sumDistinct('a)), // non-partial
      Row(0, null))
  }

  test("zero sum") {//0合计
    val emptyTableData = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(
      emptyTableData.agg(sum('a)),//df.agg() 求聚合用的相关函数
      Row(null))
  }

  test("zero sum distinct") {//不同的0合计
    val emptyTableData = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(
      emptyTableData.agg(sumDistinct('a)),
      Row(null))
  }
}
