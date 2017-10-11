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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.parquet.filter2.predicate.Operators._
import org.apache.parquet.filter2.predicate.{FilterPredicate, Operators}

import org.apache.spark.sql.{Column, DataFrame, QueryTest, Row, SQLConf}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, LogicalRelation}
import org.apache.spark.sql.test.SharedSQLContext

/**
 * A test suite that tests Parquet filter2 API based filter pushdown optimization.
 * 一个测试套件,测试基于Parquet filter2 API的过滤器下推优化
 *
 * NOTE:
 *
 * 1. `!(a cmp b)` is always transformed to its negated form `a cmp' b` by the
 *    `BooleanSimplification` optimization rule whenever possible. As a result, predicate(谓词) `!(a < 1)`
 *    results in a `GtEq` filter predicate(过虑谓词) rather than a `Not`.
 *
 * 2. `Tuple1(Option(x))` is used together with `AnyVal` types like `Int` to ensure the inferred
 *    data type is nullable.
 *    `Tuple1（Option（x））`与`AnyVal`类型一起使用,类似`Int`,以确保推断的数据类型是可空的
 */
class ParquetFilterSuite extends QueryTest with ParquetTest with SharedSQLContext {

  private def checkFilterPredicate(//过虑谓词
      df: DataFrame,
      predicate: Predicate,//谓词
      filterClass: Class[_ <: FilterPredicate],
      checker: (DataFrame, Seq[Row]) => Unit,
      expected: Seq[Row]): Unit = {
    val output = predicate.collect { case a: Attribute => a }.distinct

    withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      val query = df
        .select(output.map(e => Column(e)): _*)
        .where(Column(predicate))

      val analyzedPredicate = query.queryExecution.optimizedPlan.collect {
        case PhysicalOperation(_, filters, LogicalRelation(_: ParquetRelation, _)) => filters
      }.flatten
      assert(analyzedPredicate.nonEmpty)

      val selectedFilters = DataSourceStrategy.selectFilters(analyzedPredicate)
      assert(selectedFilters.nonEmpty)

      selectedFilters.foreach { pred =>
        val maybeFilter = ParquetFilters.createFilter(df.schema, pred)
        assert(maybeFilter.isDefined, s"Couldn't generate filter predicate for $pred")
        maybeFilter.foreach { f =>
          // Doesn't bother checking type parameters here (e.g. `Eq[Integer]`)
          //不烦恼这里检查类型参数(例如`Eq [Integer]`)
          assert(f.getClass === filterClass)
        }
      }
      checker(query, expected)
    }
  }

  private def checkFilterPredicate//检查过滤器谓词
      (predicate: Predicate, filterClass: Class[_ <: FilterPredicate], expected: Seq[Row])
      (implicit df: DataFrame): Unit = {
    checkFilterPredicate(df, predicate, filterClass, checkAnswer(_, _: Seq[Row]), expected)
  }
  //检查过滤器谓词
  private def checkFilterPredicate[T]
      (predicate: Predicate, filterClass: Class[_ <: FilterPredicate], expected: T)
      (implicit df: DataFrame): Unit = {
    checkFilterPredicate(predicate, filterClass, Seq(Row(expected)))(df)
  }
  //检查二进制过滤器谓词
  private def checkBinaryFilterPredicate
      (predicate: Predicate, filterClass: Class[_ <: FilterPredicate], expected: Seq[Row])
      (implicit df: DataFrame): Unit = {
    def checkBinaryAnswer(df: DataFrame, expected: Seq[Row]) = {
      assertResult(expected.map(_.getAs[Array[Byte]](0).mkString(",")).sorted) {
        df.map(_.getAs[Array[Byte]](0).mkString(",")).collect().toSeq.sorted
      }
    }

    checkFilterPredicate(df, predicate, filterClass, checkBinaryAnswer _, expected)
  }
  //检查二进制过滤器谓词
  private def checkBinaryFilterPredicate
      (predicate: Predicate, filterClass: Class[_ <: FilterPredicate], expected: Array[Byte])
      (implicit df: DataFrame): Unit = {
    checkBinaryFilterPredicate(predicate, filterClass, Seq(Row(expected)))(df)
  }
  //过滤下推-布尔
  test("filter pushdown - boolean") {
    //列表结尾为Nil
    withParquetDataFrame((true :: false :: Nil).map(b => Tuple1.apply(Option(b)))) { implicit df =>
      checkFilterPredicate('_1.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate('_1.isNotNull, classOf[NotEq[_]], Seq(Row(true), Row(false)))

      checkFilterPredicate('_1 === true, classOf[Eq[_]], true)
      checkFilterPredicate('_1 <=> true, classOf[Eq[_]], true)
      checkFilterPredicate('_1 !== true, classOf[NotEq[_]], false)
    }
  }
  //过滤下推 - 整数
  test("filter pushdown - integer") {
    withParquetDataFrame((1 to 4).map(i => Tuple1(Option(i)))) { implicit df =>
      checkFilterPredicate('_1.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate('_1.isNotNull, classOf[NotEq[_]], (1 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 === 1, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 <=> 1, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 !== 1, classOf[NotEq[_]], (2 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 < 2, classOf[Lt[_]], 1)
      checkFilterPredicate('_1 > 3, classOf[Gt[_]], 4)
      checkFilterPredicate('_1 <= 1, classOf[LtEq[_]], 1)
      checkFilterPredicate('_1 >= 4, classOf[GtEq[_]], 4)

      checkFilterPredicate(Literal(1) === '_1, classOf[Eq[_]], 1)
      checkFilterPredicate(Literal(1) <=> '_1, classOf[Eq[_]], 1)
      checkFilterPredicate(Literal(2) > '_1, classOf[Lt[_]], 1)
      checkFilterPredicate(Literal(3) < '_1, classOf[Gt[_]], 4)
      checkFilterPredicate(Literal(1) >= '_1, classOf[LtEq[_]], 1)
      checkFilterPredicate(Literal(4) <= '_1, classOf[GtEq[_]], 4)

      checkFilterPredicate(!('_1 < 4), classOf[GtEq[_]], 4)
      checkFilterPredicate('_1 < 2 || '_1 > 3, classOf[Operators.Or], Seq(Row(1), Row(4)))
    }
  }
  //过滤下推 - 长整型
  test("filter pushdown - long") {
    withParquetDataFrame((1 to 4).map(i => Tuple1(Option(i.toLong)))) { implicit df =>
      checkFilterPredicate('_1.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate('_1.isNotNull, classOf[NotEq[_]], (1 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 === 1, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 <=> 1, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 !== 1, classOf[NotEq[_]], (2 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 < 2, classOf[Lt[_]], 1)
      checkFilterPredicate('_1 > 3, classOf[Gt[_]], 4)
      checkFilterPredicate('_1 <= 1, classOf[LtEq[_]], 1)
      checkFilterPredicate('_1 >= 4, classOf[GtEq[_]], 4)

      checkFilterPredicate(Literal(1) === '_1, classOf[Eq[_]], 1)
      checkFilterPredicate(Literal(1) <=> '_1, classOf[Eq[_]], 1)
      checkFilterPredicate(Literal(2) > '_1, classOf[Lt[_]], 1)
      checkFilterPredicate(Literal(3) < '_1, classOf[Gt[_]], 4)
      checkFilterPredicate(Literal(1) >= '_1, classOf[LtEq[_]], 1)
      checkFilterPredicate(Literal(4) <= '_1, classOf[GtEq[_]], 4)

      checkFilterPredicate(!('_1 < 4), classOf[GtEq[_]], 4)
      checkFilterPredicate('_1 < 2 || '_1 > 3, classOf[Operators.Or], Seq(Row(1), Row(4)))
    }
  }
  //过滤器下推浮点型
  test("filter pushdown - float") {
    withParquetDataFrame((1 to 4).map(i => Tuple1(Option(i.toFloat)))) { implicit df =>
      checkFilterPredicate('_1.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate('_1.isNotNull, classOf[NotEq[_]], (1 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 === 1, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 <=> 1, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 !== 1, classOf[NotEq[_]], (2 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 < 2, classOf[Lt[_]], 1)
      checkFilterPredicate('_1 > 3, classOf[Gt[_]], 4)
      checkFilterPredicate('_1 <= 1, classOf[LtEq[_]], 1)
      checkFilterPredicate('_1 >= 4, classOf[GtEq[_]], 4)

      checkFilterPredicate(Literal(1) === '_1, classOf[Eq[_]], 1)
      checkFilterPredicate(Literal(1) <=> '_1, classOf[Eq[_]], 1)
      checkFilterPredicate(Literal(2) > '_1, classOf[Lt[_]], 1)
      checkFilterPredicate(Literal(3) < '_1, classOf[Gt[_]], 4)
      checkFilterPredicate(Literal(1) >= '_1, classOf[LtEq[_]], 1)
      checkFilterPredicate(Literal(4) <= '_1, classOf[GtEq[_]], 4)

      checkFilterPredicate(!('_1 < 4), classOf[GtEq[_]], 4)
      checkFilterPredicate('_1 < 2 || '_1 > 3, classOf[Operators.Or], Seq(Row(1), Row(4)))
    }
  }
  //过滤器下推双精度
  test("filter pushdown - double") {
    withParquetDataFrame((1 to 4).map(i => Tuple1(Option(i.toDouble)))) { implicit df =>
      checkFilterPredicate('_1.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate('_1.isNotNull, classOf[NotEq[_]], (1 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 === 1, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 <=> 1, classOf[Eq[_]], 1)
      checkFilterPredicate('_1 !== 1, classOf[NotEq[_]], (2 to 4).map(Row.apply(_)))

      checkFilterPredicate('_1 < 2, classOf[Lt[_]], 1)
      checkFilterPredicate('_1 > 3, classOf[Gt[_]], 4)
      checkFilterPredicate('_1 <= 1, classOf[LtEq[_]], 1)
      checkFilterPredicate('_1 >= 4, classOf[GtEq[_]], 4)

      checkFilterPredicate(Literal(1) === '_1, classOf[Eq[_]], 1)
      checkFilterPredicate(Literal(1) <=> '_1, classOf[Eq[_]], 1)
      checkFilterPredicate(Literal(2) > '_1, classOf[Lt[_]], 1)
      checkFilterPredicate(Literal(3) < '_1, classOf[Gt[_]], 4)
      checkFilterPredicate(Literal(1) >= '_1, classOf[LtEq[_]], 1)
      checkFilterPredicate(Literal(4) <= '_1, classOf[GtEq[_]], 4)

      checkFilterPredicate(!('_1 < 4), classOf[GtEq[_]], 4)
      checkFilterPredicate('_1 < 2 || '_1 > 3, classOf[Operators.Or], Seq(Row(1), Row(4)))
    }
  }

  // See https://issues.apache.org/jira/browse/SPARK-11153
  //过滤器下推字符串
  ignore("filter pushdown - string") {
    withParquetDataFrame((1 to 4).map(i => Tuple1(i.toString))) { implicit df =>
      checkFilterPredicate('_1.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate(
        '_1.isNotNull, classOf[NotEq[_]], (1 to 4).map(i => Row.apply(i.toString)))

      checkFilterPredicate('_1 === "1", classOf[Eq[_]], "1")
      checkFilterPredicate('_1 <=> "1", classOf[Eq[_]], "1")
      checkFilterPredicate(
        '_1 !== "1", classOf[NotEq[_]], (2 to 4).map(i => Row.apply(i.toString)))

      checkFilterPredicate('_1 < "2", classOf[Lt[_]], "1")
      checkFilterPredicate('_1 > "3", classOf[Gt[_]], "4")
      checkFilterPredicate('_1 <= "1", classOf[LtEq[_]], "1")
      checkFilterPredicate('_1 >= "4", classOf[GtEq[_]], "4")

      checkFilterPredicate(Literal("1") === '_1, classOf[Eq[_]], "1")
      checkFilterPredicate(Literal("1") <=> '_1, classOf[Eq[_]], "1")
      checkFilterPredicate(Literal("2") > '_1, classOf[Lt[_]], "1")
      checkFilterPredicate(Literal("3") < '_1, classOf[Gt[_]], "4")
      checkFilterPredicate(Literal("1") >= '_1, classOf[LtEq[_]], "1")
      checkFilterPredicate(Literal("4") <= '_1, classOf[GtEq[_]], "4")

      checkFilterPredicate(!('_1 < "4"), classOf[GtEq[_]], "4")
      checkFilterPredicate('_1 < "2" || '_1 > "3", classOf[Operators.Or], Seq(Row("1"), Row("4")))
    }
  }

  // See https://issues.apache.org/jira/browse/SPARK-11153
  //过滤器下推 - 二进制
  ignore("filter pushdown - binary") {
    implicit class IntToBinary(int: Int) {
      def b: Array[Byte] = int.toString.getBytes("UTF-8")
    }

    withParquetDataFrame((1 to 4).map(i => Tuple1(i.b))) { implicit df =>
      checkBinaryFilterPredicate('_1 === 1.b, classOf[Eq[_]], 1.b)
      checkBinaryFilterPredicate('_1 <=> 1.b, classOf[Eq[_]], 1.b)

      checkBinaryFilterPredicate('_1.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkBinaryFilterPredicate(
        '_1.isNotNull, classOf[NotEq[_]], (1 to 4).map(i => Row.apply(i.b)).toSeq)

      checkBinaryFilterPredicate(
        '_1 !== 1.b, classOf[NotEq[_]], (2 to 4).map(i => Row.apply(i.b)).toSeq)

      checkBinaryFilterPredicate('_1 < 2.b, classOf[Lt[_]], 1.b)
      checkBinaryFilterPredicate('_1 > 3.b, classOf[Gt[_]], 4.b)
      checkBinaryFilterPredicate('_1 <= 1.b, classOf[LtEq[_]], 1.b)
      checkBinaryFilterPredicate('_1 >= 4.b, classOf[GtEq[_]], 4.b)

      checkBinaryFilterPredicate(Literal(1.b) === '_1, classOf[Eq[_]], 1.b)
      checkBinaryFilterPredicate(Literal(1.b) <=> '_1, classOf[Eq[_]], 1.b)
      checkBinaryFilterPredicate(Literal(2.b) > '_1, classOf[Lt[_]], 1.b)
      checkBinaryFilterPredicate(Literal(3.b) < '_1, classOf[Gt[_]], 4.b)
      checkBinaryFilterPredicate(Literal(1.b) >= '_1, classOf[LtEq[_]], 1.b)
      checkBinaryFilterPredicate(Literal(4.b) <= '_1, classOf[GtEq[_]], 4.b)

      checkBinaryFilterPredicate(!('_1 < 4.b), classOf[GtEq[_]], 4.b)
      checkBinaryFilterPredicate(
        '_1 < 2.b || '_1 > 3.b, classOf[Operators.Or], Seq(Row(1.b), Row(4.b)))
    }
  }  
  //不要谓词下推引用分区列
  test("SPARK-6554: don't push down predicates which reference partition columns") {
    import testImplicits._

    withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      withTempPath { dir =>
        val path = s"${dir.getCanonicalPath}/part=1"
        (1 to 3).map(i => (i, i.toString)).toDF("a", "b").write.parquet(path)

        // If the "part = 1" filter gets pushed down, this query will throw an exception since
        // "part" is not a valid column in the actual Parquet file
        //如果“部分= 1”过滤器被向下推,此查询将抛出一个异常,因为“部分”是不实际的Parquet文件有效的列
        checkAnswer(
          sqlContext.read.parquet(path).filter("part = 1"),
          (1 to 3).map(i => Row(i, i.toString, 1)))
      }
    }
  }  
  //过滤器组合分区键和属性在DataSource扫描中不起作用
  test("SPARK-10829: Filter combine partition key and attribute doesn't work in DataSource scan") {
    import testImplicits._

    withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      withTempPath { dir =>
        val path = s"${dir.getCanonicalPath}/part=1"
        (1 to 3).map(i => (i, i.toString)).toDF("a", "b").write.parquet(path)

        // If the "part = 1" filter gets pushed down, this query will throw an exception since
        // "part" is not a valid column in the actual Parquet file
        //如果“part = 1”过滤器被下推,这个查询将抛出一个异常 “part”不是实际Parquet文件中的有效列
        checkAnswer(
          sqlContext.read.parquet(path).filter("a > 0 and (part = 0 or a > 1)"),
          (2 to 3).map(i => Row(i, i.toString, 1)))
      }
    }
  }  
  //应用于合并的Parquet模式与新列的筛选失败
  test("SPARK-11103: Filter applied on merged Parquet schema with new column fails") {
    import testImplicits._

    withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true",
      SQLConf.PARQUET_SCHEMA_MERGING_ENABLED.key -> "true") {
      withTempPath { dir =>
        val pathOne = s"${dir.getCanonicalPath}/table1"
        (1 to 3).map(i => (i, i.toString)).toDF("a", "b").write.parquet(pathOne)
        val pathTwo = s"${dir.getCanonicalPath}/table2"
        (1 to 3).map(i => (i, i.toString)).toDF("c", "b").write.parquet(pathTwo)

        // If the "c = 1" filter gets pushed down, this query will throw an exception which
        // Parquet emits. This is a Parquet issue (PARQUET-389).
        checkAnswer(
          sqlContext.read.parquet(pathOne, pathTwo).filter("c = 1").selectExpr("c", "b", "a"),
          (1 to 1).map(i => Row(i, i.toString, null)))
      }
    }
  }
  
  //“不”包含在Parquet过滤器下推中
  test("SPARK-12218: 'Not' is included in Parquet filter pushdown") {
    import testImplicits._

    withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      withTempPath { dir =>
        val path = s"${dir.getCanonicalPath}/table1"
        (1 to 5).map(i => (i, (i % 2).toString)).toDF("a", "b").write.parquet(path)

        checkAnswer(
          sqlContext.read.parquet(path).where("not (a = 2) or not(b in ('1'))"),
          (1 to 5).map(i => Row(i, (i % 2).toString)))

        checkAnswer(
          sqlContext.read.parquet(path).where("not (a = 2 and b in ('1'))"),
          (1 to 5).map(i => Row(i, (i % 2).toString)))
      }
    }
  }
}
