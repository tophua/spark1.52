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

package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Attribute, Literal, IsNull}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{GenericArrayData, ArrayType, StringType}
import org.apache.spark.unsafe.types.UTF8String
/**
 * 行格式化转换测试
 */
class RowFormatConvertersSuite extends SparkPlanTest with SharedSQLContext {

  private def getConverters(plan: SparkPlan): Seq[SparkPlan] = plan.collect {
    //转换不安全
    case c: ConvertToUnsafe => c
    //转换安全
    case c: ConvertToSafe => c
  }
 
  private val outputsSafe = ExternalSort(Nil, false, PhysicalRDD(Seq.empty, null, "name"))
  assert(!outputsSafe.outputsUnsafeRows)
  private val outputsUnsafe = TungstenSort(Nil, false, PhysicalRDD(Seq.empty, null, "name"))
  assert(outputsUnsafe.outputsUnsafeRows)
  //计划在需要时插入不安全的安全转换
  test("planner should insert unsafe->safe conversions when required") {
    val plan = Limit(10, outputsUnsafe)
    val preparedPlan = ctx.prepareForExecution.execute(plan)
    assert(preparedPlan.children.head.isInstanceOf[ConvertToSafe])
  }
  //过滤器可以处理不安全的行
  test("filter can process unsafe rows") {
    val plan = Filter(IsNull(IsNull(Literal(1))), outputsUnsafe)
    val preparedPlan = ctx.prepareForExecution.execute(plan)
    assert(getConverters(preparedPlan).size === 1)
    assert(preparedPlan.outputsUnsafeRows)
  }
  //过滤器可以处理安全行
  test("filter can process safe rows") {
    val plan = Filter(IsNull(IsNull(Literal(1))), outputsSafe)
    val preparedPlan = ctx.prepareForExecution.execute(plan)
    assert(getConverters(preparedPlan).isEmpty)
    assert(!preparedPlan.outputsUnsafeRows)
  }
  //execute()断言失败如果输入行不同的格式
  test("execute() fails an assertion if inputs rows are of different formats") {
    val e = intercept[AssertionError] {
      Union(Seq(outputsSafe, outputsUnsafe)).execute()
    }
    assert(e.getMessage.contains("format"))
  }
  //联合要求所有的输入行格式一致
  test("union requires all of its input rows' formats to agree") {
    val plan = Union(Seq(outputsSafe, outputsUnsafe))
    assert(plan.canProcessSafeRows && plan.canProcessUnsafeRows)
    val preparedPlan = ctx.prepareForExecution.execute(plan)
    assert(preparedPlan.outputsUnsafeRows)
  }
  //可以处理安全行
  test("union can process safe rows") {
    val plan = Union(Seq(outputsSafe, outputsSafe))
    val preparedPlan = ctx.prepareForExecution.execute(plan)
    assert(!preparedPlan.outputsUnsafeRows)
  }
 //可以处理不安全行
  test("union can process unsafe rows") {
    val plan = Union(Seq(outputsUnsafe, outputsUnsafe))
    val preparedPlan = ctx.prepareForExecution.execute(plan)
    assert(preparedPlan.outputsUnsafeRows)
  }
  //返转换到不安全和转换为安全
  test("round trip with ConvertToUnsafe and ConvertToSafe") {
    val input = Seq(("hello", 1), ("world", 2))
    checkAnswer(
      ctx.createDataFrame(input),
      plan => ConvertToSafe(ConvertToUnsafe(plan)),
      input.map(Row.fromTuple)
    )
  }
  //当转换为UTF8字符串复制阵列/地图安全不安全
  test("SPARK-9683: copy UTF8String when convert unsafe array/map to safe") {
    SparkPlan.currentContext.set(ctx)
    val schema = ArrayType(StringType)
    val rows = (1 to 100).map { i =>
      InternalRow(new GenericArrayData(Array[Any](UTF8String.fromString(i.toString))))
    }
    val relation = LocalTableScan(Seq(AttributeReference("t", schema)()), rows)

    val plan =
      DummyPlan(
        ConvertToSafe(
          ConvertToUnsafe(relation)))
    assert(plan.execute().collect().map(_.getUTF8String(0).toString) === (1 to 100).map(_.toString))
  }
}
//假计划
case class DummyPlan(child: SparkPlan) extends UnaryNode {

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitions { iter =>
      //这个“虚拟计划”处于安全模式,所以我们不需要做拷贝,即使我们持有一些从传入行得到的值
      // This `DummyPlan` is in safe mode, so we don't need to do copy even we hold some
      // values gotten from the incoming rows.
      // we cache all strings here to make sure we have deep copied UTF8String inside incoming
      // safe InternalRow.
      //我们的高速缓存中的所有字符串来确保我们有深复制UTF8字符串里面输入安全的内部行
      val strings = new scala.collection.mutable.ArrayBuffer[UTF8String]
      iter.foreach { row =>
        strings += row.getArray(0).getUTF8String(0)
      }
      strings.map(InternalRow(_)).iterator
    }
  }

  override def output: Seq[Attribute] = Seq(AttributeReference("a", StringType)())
}
