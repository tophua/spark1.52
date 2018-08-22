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

import java.io.OutputStream
import java.util.{List => JList, Map => JMap}

import scala.collection.JavaConversions._

import net.razorvine.pickle._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.python.{PythonRunner, PythonBroadcast, PythonRDD, SerDeUtil}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{Logging => SparkLogging, TaskContext, Accumulator}

/**
 * A serialized version of a Python lambda function.  Suitable for use in a [[PythonRDD]].
  * Python lambda函数的序列化版本,适用于[[PythonRDD]]
 */
private[spark] case class PythonUDF(
    name: String,
    command: Array[Byte],
    envVars: JMap[String, String],
    pythonIncludes: JList[String],
    pythonExec: String,
    pythonVer: String,
    broadcastVars: JList[Broadcast[PythonBroadcast]],
    accumulator: Accumulator[JList[Array[Byte]]],
    dataType: DataType,
    children: Seq[Expression]) extends Expression with Unevaluable with SparkLogging {

  override def toString: String = s"PythonUDF#$name(${children.mkString(",")})"

  override def nullable: Boolean = true
}

/**
 * Extracts PythonUDFs from operators, rewriting the query plan so that the UDF can be evaluated
 * alone in a batch.
  * 从运算符中提取PythonUDF,重写查询计划,以便可以批量单独评估UDF
 *
 * This has the limitation that the input to the Python UDF is not allowed include attributes from
 * multiple child operators.
  * 这具有以下限制：不允许对Python UDF的输入包括来自多个子运算符的属性
 */
private[spark] object ExtractPythonUDFs extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    // Skip EvaluatePython nodes.
    //跳过EvaluatePython节点
    case plan: EvaluatePython => plan

    case plan: LogicalPlan if plan.resolved =>
      // Extract any PythonUDFs from the current operator.
      //从当前运算符中提取任何PythonUDF
      val udfs = plan.expressions.flatMap(_.collect { case udf: PythonUDF => udf })
      if (udfs.isEmpty) {
        // If there aren't any, we are done.
        //如果没有,我们就完成了
        plan
      } else {
        // Pick the UDF we are going to evaluate (TODO: Support evaluating multiple UDFs at a time)
        // If there is more than one, we will add another evaluation operator in a subsequent pass.
        //如果有多个,我们将在后续传递中添加另一个评估运算符
        udfs.find(_.resolved) match {
          case Some(udf) =>
            var evaluation: EvaluatePython = null

            // Rewrite the child that has the input required for the UDF
            //重写具有UDF所需输入的子级
            val newChildren = plan.children.map { child =>
              // Check to make sure that the UDF can be evaluated with only the input of this child.
              //检查以确保只使用此子项的输入来评估UDF
              // Other cases are disallowed as they are ambiguous or would require a cartesian
              // product.
              //其他情况是不允许的,因为它们含糊不清或需要笛卡尔积
              if (udf.references.subsetOf(child.outputSet)) {
                evaluation = EvaluatePython(udf, child)
                evaluation
              } else if (udf.references.intersect(child.outputSet).nonEmpty) {
                sys.error(s"Invalid PythonUDF $udf, requires attributes from more than one child.")
              } else {
                child
              }
            }

            assert(evaluation != null, "Unable to evaluate PythonUDF.  Missing input attributes.")

            // Trim away the new UDF value if it was only used for filtering or something.
            //如果新UDF值仅用于过滤或其他内容,则将其删除
            logical.Project(
              plan.output,
              plan.transformExpressions {
                case p: PythonUDF if p.fastEquals(udf) => evaluation.resultAttribute
              }.withNewChildren(newChildren))

          case None =>
            // If there is no Python UDF that is resolved, skip this round.
            //如果没有解析的Python UDF,请跳过此轮
            plan
        }
      }
  }
}

object EvaluatePython {
  def apply(udf: PythonUDF, child: LogicalPlan): EvaluatePython =
    new EvaluatePython(udf, child, AttributeReference("pythonUDF", udf.dataType)())

  def takeAndServe(df: DataFrame, n: Int): Int = {
    registerPicklers()
    // This is an annoying hack - we should refactor the code so executeCollect and executeTake
    // returns InternalRow rather than Row.
    //这是一个烦人的黑客 - 我们应该重构代码,所以executeCollect和executeTake返回InternalRow而不是Row
    val converter = CatalystTypeConverters.createToCatalystConverter(df.schema)
    val iter = new SerDeUtil.AutoBatchedPickler(df.take(n).iterator.map { row =>
      EvaluatePython.toJava(converter(row).asInstanceOf[InternalRow], df.schema)
    })
    PythonRDD.serveIterator(iter, s"serve-DataFrame")
  }

  /**
   * Helper for converting from Catalyst type to java type suitable for Pyrolite.
    * 用于从Catalyst类型转换为适用于Pyrolite的java类型的Helper
   */
  def toJava(obj: Any, dataType: DataType): Any = (obj, dataType) match {
    case (null, _) => null

    case (row: InternalRow, struct: StructType) =>
      val values = new Array[Any](row.numFields)
      var i = 0
      while (i < row.numFields) {
        values(i) = toJava(row.get(i, struct.fields(i).dataType), struct.fields(i).dataType)
        i += 1
      }
      new GenericInternalRowWithSchema(values, struct)

    case (a: ArrayData, array: ArrayType) =>
      val values = new java.util.ArrayList[Any](a.numElements())
      a.foreach(array.elementType, (_, e) => {
        values.add(toJava(e, array.elementType))
      })
      values

    case (map: MapData, mt: MapType) =>
      val jmap = new java.util.HashMap[Any, Any](map.numElements())
      map.foreach(mt.keyType, mt.valueType, (k, v) => {
        jmap.put(toJava(k, mt.keyType), toJava(v, mt.valueType))
      })
      jmap

    case (ud, udt: UserDefinedType[_]) => toJava(ud, udt.sqlType)

    case (d: Decimal, _) => d.toJavaBigDecimal

    case (s: UTF8String, StringType) => s.toString

    case (other, _) => other
  }

  /**
   * Converts `obj` to the type specified by the data type, or returns null if the type of obj is
   * unexpected. Because Python doesn't enforce the type.
    * 将`obj`转换为数据类型指定的类型,如果obj的类型是意外的,则返回null,因为Python不强制执行该类型
   */
  def fromJava(obj: Any, dataType: DataType): Any = (obj, dataType) match {
    case (null, _) => null

    case (c: Boolean, BooleanType) => c

    case (c: Int, ByteType) => c.toByte
    case (c: Long, ByteType) => c.toByte

    case (c: Int, ShortType) => c.toShort
    case (c: Long, ShortType) => c.toShort

    case (c: Int, IntegerType) => c
    case (c: Long, IntegerType) => c.toInt

    case (c: Int, LongType) => c.toLong
    case (c: Long, LongType) => c

    case (c: Double, FloatType) => c.toFloat

    case (c: Double, DoubleType) => c

    case (c: java.math.BigDecimal, dt: DecimalType) => Decimal(c, dt.precision, dt.scale)

    case (c: Int, DateType) => c

    case (c: Long, TimestampType) => c

    case (c: String, StringType) => UTF8String.fromString(c)
    case (c, StringType) =>
      // If we get here, c is not a string. Call toString on it.
      //如果我们到这里,c不是一个字符串,在上面调用toString
      UTF8String.fromString(c.toString)

    case (c: String, BinaryType) => c.getBytes("utf-8")
    case (c, BinaryType) if c.getClass.isArray && c.getClass.getComponentType.getName == "byte" => c

    case (c: java.util.List[_], ArrayType(elementType, _)) =>
      new GenericArrayData(c.map { e => fromJava(e, elementType)}.toArray)

    case (c, ArrayType(elementType, _)) if c.getClass.isArray =>
      new GenericArrayData(c.asInstanceOf[Array[_]].map(e => fromJava(e, elementType)))

    case (c: java.util.Map[_, _], MapType(keyType, valueType, _)) =>
      val keys = c.keysIterator.map(fromJava(_, keyType)).toArray
      val values = c.valuesIterator.map(fromJava(_, valueType)).toArray
      ArrayBasedMapData(keys, values)

    case (c, StructType(fields)) if c.getClass.isArray =>
      new GenericInternalRow(c.asInstanceOf[Array[_]].zip(fields).map {
        case (e, f) => fromJava(e, f.dataType)
      })

    case (_, udt: UserDefinedType[_]) => fromJava(obj, udt.sqlType)

    // all other unexpected type should be null, or we will have runtime exception
      //所有其他意外类型应为null,否则我们将有运行时异常
    // TODO(davies): we could improve this by try to cast the object to expected type
    case (c, _) => null
  }


  private val module = "pyspark.sql.types"

  /**
   * Pickler for StructType
   */
  private class StructTypePickler extends IObjectPickler {

    private val cls = classOf[StructType]

    def register(): Unit = {
      Pickler.registerCustomPickler(cls, this)
    }

    def pickle(obj: Object, out: OutputStream, pickler: Pickler): Unit = {
      out.write(Opcodes.GLOBAL)
      out.write((module + "\n" + "_parse_datatype_json_string" + "\n").getBytes("utf-8"))
      val schema = obj.asInstanceOf[StructType]
      pickler.save(schema.json)
      out.write(Opcodes.TUPLE1)
      out.write(Opcodes.REDUCE)
    }
  }

  /**
   * Pickler for InternalRow
   */
  private class RowPickler extends IObjectPickler {

    private val cls = classOf[GenericInternalRowWithSchema]

    // register this to Pickler and Unpickler
    //将此注册到Pickler和Unpickler
    def register(): Unit = {
      Pickler.registerCustomPickler(this.getClass, this)
      Pickler.registerCustomPickler(cls, this)
    }

    def pickle(obj: Object, out: OutputStream, pickler: Pickler): Unit = {
      if (obj == this) {
        out.write(Opcodes.GLOBAL)
        out.write((module + "\n" + "_create_row_inbound_converter" + "\n").getBytes("utf-8"))
      } else {
        // it will be memorized by Pickler to save some bytes
        pickler.save(this)
        val row = obj.asInstanceOf[GenericInternalRowWithSchema]
        // schema should always be same object for memoization
        pickler.save(row.schema)
        out.write(Opcodes.TUPLE1)
        out.write(Opcodes.REDUCE)

        out.write(Opcodes.MARK)
        var i = 0
        while (i < row.values.size) {
          pickler.save(row.values(i))
          i += 1
        }
        out.write(Opcodes.TUPLE)
        out.write(Opcodes.REDUCE)
      }
    }
  }

  private[this] var registered = false
  /**
   * This should be called before trying to serialize any above classes un cluster mode,
   * this should be put in the closure
    * 这应该在尝试序列化任何上面的类un cluster模式之前调用,这应该放在闭包中
   */
  def registerPicklers(): Unit = {
    synchronized {
      if (!registered) {
        SerDeUtil.initialize()
        new StructTypePickler().register()
        new RowPickler().register()
        registered = true
      }
    }
  }

  /**
   * Convert an RDD of Java objects to an RDD of serialized Python objects, that is usable by
   * PySpark.
    * 将Java对象的RDD转换为可由PySpark使用的序列化Python对象的RDD
   */
  def javaToPython(rdd: RDD[Any]): RDD[Array[Byte]] = {
    rdd.mapPartitions { iter =>
      registerPicklers()  // let it called in executor
      new SerDeUtil.AutoBatchedPickler(iter)
    }
  }
}

/**
 * :: DeveloperApi ::
 * Evaluates a [[PythonUDF]], appending the result to the end of the input tuple.
  * 计算[[PythonUDF]],将结果追加到输入元组的末尾
 */
@DeveloperApi
case class EvaluatePython(
    udf: PythonUDF,
    child: LogicalPlan,
    resultAttribute: AttributeReference)
  extends logical.UnaryNode {

  def output: Seq[Attribute] = child.output :+ resultAttribute

  // References should not include the produced attribute.
  override def references: AttributeSet = udf.references
}

/**
 * :: DeveloperApi ::
 * Uses PythonRDD to evaluate a [[PythonUDF]], one partition of tuples at a time.
  * 使用PythonRDD来评估[[PythonUDF]],一次一个元组分区
 *
 * Python evaluation works by sending the necessary (projected) input data via a socket to an
 * external Python process, and combine the result from the Python process with the original row.
  *
  * Python评估的工作原理是通过套接字将必要的(投影的)输入数据发送到外部Python进程,并将Python进程的结果与原始行结合起来。
 *
 * For each row we send to Python, we also put it in a queue. For each output row from Python,
 * we drain the queue to find the original input row. Note that if the Python process is way too
 * slow, this could lead to the queue growing unbounded and eventually run out of memory.
  *
  * 对于我们发送给Python的每一行,我们也将它放入队列中,对于Python中的每个输出行,我们排空队列以查找原始输入行。
  * 请注意,如果Python进程太慢,这可能会导致队列无限制地增长并最终耗尽内存
 */
@DeveloperApi
case class BatchPythonEvaluation(udf: PythonUDF, output: Seq[Attribute], child: SparkPlan)
  extends SparkPlan {

  def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val inputRDD = child.execute().map(_.copy())
    val bufferSize = inputRDD.conf.getInt("spark.buffer.size", 65536)
    val reuseWorker = inputRDD.conf.getBoolean("spark.python.worker.reuse", defaultValue = true)

    inputRDD.mapPartitions { iter =>
      EvaluatePython.registerPicklers()  // register pickler for Row

      // The queue used to buffer input rows so we can drain it to
      // combine input with output from Python.
      //该队列用于缓冲输入行,因此我们可以将其排出以将输入与Python的输出结合起来
      val queue = new java.util.concurrent.ConcurrentLinkedQueue[InternalRow]()

      val pickle = new Pickler
      val currentRow = newMutableProjection(udf.children, child.output)()
      val fields = udf.children.map(_.dataType)
      val schema = new StructType(fields.map(t => new StructField("", t, true)).toArray)

      // Input iterator to Python: input rows are grouped so we send them in batches to Python.
      // For each row, add it to the queue.
      //将输入迭代器输入到Python：输入行被分组,因此我们将它们批量发送到Python,对于每一行,将其添加到队列中。
      val inputIterator = iter.grouped(100).map { inputRows =>
        val toBePickled = inputRows.map { row =>
          queue.add(row)
          EvaluatePython.toJava(currentRow(row), schema)
        }.toArray
        pickle.dumps(toBePickled)
      }

      val context = TaskContext.get()

      // Output iterator for results from Python.
      //输出迭代器以获取Python的结果
      val outputIterator = new PythonRunner(
        udf.command,
        udf.envVars,
        udf.pythonIncludes,
        udf.pythonExec,
        udf.pythonVer,
        udf.broadcastVars,
        udf.accumulator,
        bufferSize,
        reuseWorker
      ).compute(inputIterator, context.partitionId(), context)

      val unpickle = new Unpickler
      val row = new GenericMutableRow(1)
      val joined = new JoinedRow

      outputIterator.flatMap { pickedResult =>
        val unpickledBatch = unpickle.loads(pickedResult)
        unpickledBatch.asInstanceOf[java.util.ArrayList[Any]]
      }.map { result =>
        row(0) = EvaluatePython.fromJava(result, udf.dataType)
        joined(queue.poll(), row)
      }
    }
  }
}
