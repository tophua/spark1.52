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

import org.apache.spark.sql.execution.joins.BroadcastHashJoin
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext

class DataFrameJoinSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("join - join using") {//使用连接
     /**
     * df.show()
      +---+---+
      |int|str|
      +---+---+
      |  1|  1|
      |  2|  2|
      |  3|  3|
      +---+---+*/
    val df = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str")
   /**
    	df2.show()
      +---+---+
      |int|str|
      +---+---+
      |  1|  2|
      |  2|  3|
      |  3|  4|
      +---+---+*/
    val df2 = Seq(1, 2, 3).map(i => (i, (i + 1).toString)).toDF("int", "str")    
    /**
    df.join(df2, "int").show()
    +---+---+---+
    |int|str|str|
    +---+---+---+
    |  1|  1|  2|
    |  2|  2|  3|
    |  3|  3|  4|
    +---+---+---+*/    
    checkAnswer(
      //使用内连接
      df.join(df2, "int"),
      Row(1, "1", "2") :: Row(2, "2", "3") :: Row(3, "3", "4") :: Nil)
  }

  test("join - join using multiple columns") {//加入使用多个列
    val df = Seq(1, 2, 3).map(i => (i, i + 1, i.toString)).toDF("int", "int2", "str")
    val df2 = Seq(1, 2, 3).map(i => (i, i + 1, (i + 1).toString)).toDF("int", "int2", "str")

    checkAnswer(//使用多列内连接
      df.join(df2, Seq("int", "int2")),
      Row(1, 2, "1", "2") :: Row(2, 3, "2", "3") :: Row(3, 4, "3", "4") :: Nil)
  }

  test("join - join using self join") {//加入使用自连接
    val df = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str")

    // self join
    checkAnswer(//自连接
      df.join(df, "int"),
      Row(1, "1", "1") :: Row(2, "2", "2") :: Row(3, "3", "3") :: Nil)
  }

  test("join - self join") {//自连接
    val df1 = testData.select(testData("key")).as('df1)
    val df2 = testData.select(testData("key")).as('df2)

    checkAnswer(
      df1.join(df2, $"df1.key" === $"df2.key"),
      sql("SELECT a.key, b.key FROM testData a JOIN testData b ON a.key = b.key")
        .collect().toSeq)
  }

  test("join - using aliases after self join") {//自连接后使用别名
    val df = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str")
    checkAnswer(
      df.as('x).join(df.as('y), $"x.str" === $"y.str").groupBy("x.str").count(),
      Row("1", 1) :: Row("2", 1) :: Row("3", 1) :: Nil)

    checkAnswer(
      df.as('x).join(df.as('y), $"x.str" === $"y.str").groupBy("y.str").count(),
      Row("1", 1) :: Row("2", 1) :: Row("3", 1) :: Nil)
  }

  test("[SPARK-6231] join - self join auto resolve ambiguity") {//自连接自动解决歧义
    val df = Seq((1, "1"), (2, "2")).toDF("key", "value")
    checkAnswer(
      df.join(df, df("key") === df("key")),
      Row(1, "1", 1, "1") :: Row(2, "2", 2, "2") :: Nil)

    checkAnswer(
      df.join(df.filter($"value" === "2"), df("key") === df("key")),
      Row(2, "2", 2, "2") :: Nil)

    checkAnswer(
      df.join(df, df("key") === df("key") && df("value") === 1),
      Row(1, "1", 1, "1") :: Nil)

    val left = df.groupBy("key").agg(count("*"))//df.agg() 求聚合用的相关函数
    val right = df.groupBy("key").agg(sum("key"))//df.agg() 求聚合用的相关函数
    checkAnswer(
      left.join(right, left("key") === right("key")),
      Row(1, 1, 1, 1) :: Row(2, 1, 2, 2) :: Nil)
  }

  test("broadcast join hint") {//广播连接提示
    val df1 = Seq((1, "1"), (2, "2")).toDF("key", "value")
    val df2 = Seq((1, "1"), (2, "2")).toDF("key", "value")

    // equijoin - should be converted into broadcast join
    //equijoin-应转换成广播连接
    val plan1 = df1.join(broadcast(df2), "key").queryExecution.executedPlan
    assert(plan1.collect { case p: BroadcastHashJoin => p }.size === 1)

    // no join key -- should not be a broadcast join
    //没有加入键-不应该是一个广播连接
    val plan2 = df1.join(broadcast(df2)).queryExecution.executedPlan
    assert(plan2.collect { case p: BroadcastHashJoin => p }.size === 0)

    // planner should not crash without a join
    broadcast(df1).queryExecution.executedPlan

    // SPARK-12275: no physical plan for BroadcastHint in some condition
    withTempPath { path =>
      df1.write.parquet(path.getCanonicalPath)
      val pf1 = sqlContext.read.parquet(path.getCanonicalPath)
      assert(df1.join(broadcast(pf1)).count() === 4)
    }
  }
}
