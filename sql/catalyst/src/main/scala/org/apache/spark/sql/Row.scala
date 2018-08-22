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

import scala.util.hashing.MurmurHash3

import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.StructType

object Row {
  /**
   * This method can be used to extract fields from a [[Row]] object in a pattern match. Example:
    * 此方法可用于从模式匹配中的[ROW]对象中提取字段,例子：
   * {{{
   * import org.apache.spark.sql._
   *
   * val pairs = sql("SELECT key, value FROM src").rdd.map {
   *   case Row(key: Int, value: String) =>
   *     key -> value
   * }
   * }}}
   */
  def unapplySeq(row: Row): Some[Seq[Any]] = Some(row.toSeq)

  /**
   * This method can be used to construct a [[Row]] with the given values.
    * 该方法可用于构造具有给定值的[ROW]
   */
  def apply(values: Any*): Row = new GenericRow(values.toArray)

  /**
   * This method can be used to construct a [[Row]] from a [[Seq]] of values.
    * 此方法可用于从[SEQ]值构造[ROW]。
   */
  def fromSeq(values: Seq[Any]): Row = new GenericRow(values.toArray)

  def fromTuple(tuple: Product): Row = fromSeq(tuple.productIterator.toSeq)

  /**
   * Merge multiple rows into a single row, one after another.
    * 将多行合并为一行,一行接一行
   */
  def merge(rows: Row*): Row = {
    // TODO: Improve the performance of this if used in performance critical part.
    new GenericRow(rows.flatMap(_.toSeq).toArray)
  }

  /** Returns an empty row. */
  val empty = apply()
}


/**
 * Represents one row of output from a relational operator.  Allows both generic access by ordinal,
 * which will incur boxing overhead for primitives, as well as native primitive access.
  * 表示关系操作符的一行输出,允许序列化的通用访问,这将引发原语的装箱开销,以及本地原语访问。
 *
 * It is invalid to use the native primitive interface to retrieve a value that is null, instead a
 * user must check `isNullAt` before attempting to retrieve a value that might be null.
 *
  * 使用本机原始接口检索一个值为NULL是无效的,相反,用户必须在尝试检索可能为NULL的值之前检查“ISNulLAT”
  *
 * To create a new Row, use [[RowFactory.create()]] in Java or [[Row.apply()]] in Scala.
 *
 * A [[Row]] object can be constructed by providing field values. Example:
 * {{{
 * import org.apache.spark.sql._
 *
 * // Create a Row from values.
 * Row(value1, value2, value3, ...)
 * // Create a Row from a Seq of values.
 * Row.fromSeq(Seq(value1, value2, ...))
 * }}}
 *
 * A value of a row can be accessed through both generic access by ordinal,
 * which will incur boxing overhead for primitives, as well as native primitive access.
 * An example of generic access by ordinal:
 * {{{
 * import org.apache.spark.sql._
 *
 * val row = Row(1, true, "a string", null)
 * // row: Row = [1,true,a string,null]
 * val firstValue = row(0)
 * // firstValue: Any = 1
 * val fourthValue = row(3)
 * // fourthValue: Any = null
 * }}}
 *
 * For native primitive access, it is invalid to use the native primitive interface to retrieve
 * a value that is null, instead a user must check `isNullAt` before attempting to retrieve a
 * value that might be null.
 * An example of native primitive access:
 * {{{
 * // using the row from the previous example.
 * val firstValue = row.getInt(0)
 * // firstValue: Int = 1
 * val isNull = row.isNullAt(3)
 * // isNull: Boolean = true
 * }}}
 *
 * In Scala, fields in a [[Row]] object can be extracted in a pattern match. Example:
  * 在Scala中,可以在模式匹配中提取[ROW]对象中的字段。例子：
 * {{{
 * import org.apache.spark.sql._
 *
 * val pairs = sql("SELECT key, value FROM src").rdd.map {
 *   case Row(key: Int, value: String) =>
 *     key -> value
 * }
 * }}}
 *
 * @group row
 */
trait Row extends Serializable {
  /** Number of elements in the Row.
    * 行中的元素数*/
  def size: Int = length

  /** Number of elements in the Row.
    * 行中的元素数。*/
  def length: Int

  /**
   * Schema for the row.
    * 行的模式
   */
  def schema: StructType = null

  /**
   * Returns the value at position i. If the value is null, null is returned. The following
   * is a mapping between Spark SQL types and return types:
   * 返回位置i的值,如果值为null,则返回null,以下是Spark SQL类型和返回类型之间的映射：
   * {{{
   *   BooleanType -> java.lang.Boolean
   *   ByteType -> java.lang.Byte
   *   ShortType -> java.lang.Short
   *   IntegerType -> java.lang.Integer
   *   FloatType -> java.lang.Float
   *   DoubleType -> java.lang.Double
   *   StringType -> String
   *   DecimalType -> java.math.BigDecimal
   *
   *   DateType -> java.sql.Date
   *   TimestampType -> java.sql.Timestamp
   *
   *   BinaryType -> byte array
   *   ArrayType -> scala.collection.Seq (use getList for java.util.List)
   *   MapType -> scala.collection.Map (use getJavaMap for java.util.Map)
   *   StructType -> org.apache.spark.sql.Row
   * }}}
   */
  def apply(i: Int): Any = get(i)

  /**
   * Returns the value at position i. If the value is null, null is returned. The following
   * is a mapping between Spark SQL types and return types:
    * 返回位置i的值,如果值为null,则返回null,以下是Spark SQL类型和返回类型之间的映射：
   *
   * {{{
   *   BooleanType -> java.lang.Boolean
   *   ByteType -> java.lang.Byte
   *   ShortType -> java.lang.Short
   *   IntegerType -> java.lang.Integer
   *   FloatType -> java.lang.Float
   *   DoubleType -> java.lang.Double
   *   StringType -> String
   *   DecimalType -> java.math.BigDecimal
   *
   *   DateType -> java.sql.Date
   *   TimestampType -> java.sql.Timestamp
   *
   *   BinaryType -> byte array
   *   ArrayType -> scala.collection.Seq (use getList for java.util.List)
   *   MapType -> scala.collection.Map (use getJavaMap for java.util.Map)
   *   StructType -> org.apache.spark.sql.Row
   * }}}
   */
  def get(i: Int): Any

  /** Checks whether the value at position i is null.
    * 检查位置i的值是否为空*/
  def isNullAt(i: Int): Boolean = get(i) == null

  /**
   * Returns the value at position i as a primitive boolean.
    * 返回位置i的值作为基本布尔值
   *
   * @throws ClassCastException when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getBoolean(i: Int): Boolean = getAs[Boolean](i)

  /**
   * Returns the value at position i as a primitive byte.
    * 返回位置i的值作为基本字节
   *
   * @throws ClassCastException when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getByte(i: Int): Byte = getAs[Byte](i)

  /**
   * Returns the value at position i as a primitive short.
    * 返回位置i的值作为原始short
   *
   * @throws ClassCastException when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getShort(i: Int): Short = getAs[Short](i)

  /**
   * Returns the value at position i as a primitive int.
   * 返回位置i的值作为原始int
   * @throws ClassCastException when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getInt(i: Int): Int = getAs[Int](i)

  /**
   * Returns the value at position i as a primitive long.
    * 返回位置i的值作为原始long
   *
   * @throws ClassCastException when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getLong(i: Int): Long = getAs[Long](i)

  /**
   * Returns the value at position i as a primitive float.
    * 返回位置i的值作为原始float。
   * Throws an exception if the type mismatches or if the value is null.
    * 如果类型不匹配或值为null,则抛出异常
   *
   * @throws ClassCastException when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getFloat(i: Int): Float = getAs[Float](i)

  /**
   * Returns the value at position i as a primitive double.
    * 返回位置i的值作为基本double
   *
   * @throws ClassCastException when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getDouble(i: Int): Double = getAs[Double](i)

  /**
   * Returns the value at position i as a String object.
    * 返回位置i的值作为String对象
   *
   * @throws ClassCastException when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getString(i: Int): String = getAs[String](i)

  /**
   * Returns the value at position i of decimal type as java.math.BigDecimal.
   * 返回十进制类型的位置i的值为java.math.BigDecimal
   * @throws ClassCastException when data type does not match.
   */
  def getDecimal(i: Int): java.math.BigDecimal = getAs[java.math.BigDecimal](i)

  /**
   * Returns the value at position i of date type as java.sql.Date.
    * 将日期类型的位置i处的值返回为java.sql.Date
   *
   * @throws ClassCastException when data type does not match.
   */
  def getDate(i: Int): java.sql.Date = getAs[java.sql.Date](i)

  /**
   * Returns the value at position i of date type as java.sql.Timestamp.
    * 将日期类型的位置i处的值返回为java.sql.Timestamp
   *
   * @throws ClassCastException when data type does not match.
   */
  def getTimestamp(i: Int): java.sql.Timestamp = getAs[java.sql.Timestamp](i)

  /**
   * Returns the value at position i of array type as a Scala Seq.
    * 返回数组类型的位置i处的值作为Scala Seq
   *
   * @throws ClassCastException when data type does not match.
   */
  def getSeq[T](i: Int): Seq[T] = getAs[Seq[T]](i)

  /**
   * Returns the value at position i of array type as [[java.util.List]].
    * 将数组类型的位置i处的值返回为[[java.util.List]]
   *
   * @throws ClassCastException when data type does not match.
   */
  def getList[T](i: Int): java.util.List[T] = {
    scala.collection.JavaConversions.seqAsJavaList(getSeq[T](i))
  }

  /**
   * Returns the value at position i of map type as a Scala Map.
    * 返回Map类型的位置i处的值作为Scala Map。
   *
   * @throws ClassCastException when data type does not match.
   */
  def getMap[K, V](i: Int): scala.collection.Map[K, V] = getAs[Map[K, V]](i)

  /**
   * Returns the value at position i of array type as a [[java.util.Map]].
    * 返回数组类型的位置i的值为[[java.util.Map]]
   *
   * @throws ClassCastException when data type does not match.
   */
  def getJavaMap[K, V](i: Int): java.util.Map[K, V] = {
    scala.collection.JavaConversions.mapAsJavaMap(getMap[K, V](i))
  }

  /**
   * Returns the value at position i of struct type as an [[Row]] object.
    * 返回struct类型的位置i处的值作为[[Row]]对象
   *
   * @throws ClassCastException when data type does not match.
   */
  def getStruct(i: Int): Row = getAs[Row](i)

  /**
   * Returns the value at position i.
    * 返回位置i的值
   *
   * @throws ClassCastException when data type does not match.
   */
  def getAs[T](i: Int): T = get(i).asInstanceOf[T]

  /**
   * Returns the value of a given fieldName.
    * 返回给定fieldName的值
   *
   * @throws UnsupportedOperationException when schema is not defined.
   * @throws IllegalArgumentException when fieldName do not exist.
   * @throws ClassCastException when data type does not match.
   */
  def getAs[T](fieldName: String): T = getAs[T](fieldIndex(fieldName))

  /**
   * Returns the index of a given field name.
    * 返回给定字段名称的索引
   *
   * @throws UnsupportedOperationException when schema is not defined.
   * @throws IllegalArgumentException when fieldName do not exist.
   */
  def fieldIndex(name: String): Int = {
    throw new UnsupportedOperationException("fieldIndex on a Row without schema is undefined.")
  }

  /**
   * Returns a Map(name -> value) for the requested fieldNames
    * 返回请求的fieldNames的Map（名称 - >值）
   *
   * @throws UnsupportedOperationException when schema is not defined.
   * @throws IllegalArgumentException when fieldName do not exist.
   * @throws ClassCastException when data type does not match.
   */
  def getValuesMap[T](fieldNames: Seq[String]): Map[String, T] = {
    fieldNames.map { name =>
      name -> getAs[T](name)
    }.toMap
  }

  override def toString(): String = s"[${this.mkString(",")}]"

  /**
   * Make a copy of the current [[Row]] object.
    * 制作当前[[Row]]对象的副本
   */
  def copy(): Row

  /** Returns true if there are any NULL values in this row.
    * 如果此行中有任何NULL值,则返回true*/
  def anyNull: Boolean = {
    val len = length
    var i = 0
    while (i < len) {
      if (isNullAt(i)) { return true }
      i += 1
    }
    false
  }

  override def equals(o: Any): Boolean = {
    if (!o.isInstanceOf[Row]) return false
    val other = o.asInstanceOf[Row]

    if (other eq null) return false

    if (length != other.length) {
      return false
    }

    var i = 0
    while (i < length) {
      if (isNullAt(i) != other.isNullAt(i)) {
        return false
      }
      if (!isNullAt(i)) {
        val o1 = get(i)
        val o2 = other.get(i)
        o1 match {
          case b1: Array[Byte] =>
            if (!o2.isInstanceOf[Array[Byte]] ||
                !java.util.Arrays.equals(b1, o2.asInstanceOf[Array[Byte]])) {
              return false
            }
          case f1: Float if java.lang.Float.isNaN(f1) =>
            if (!o2.isInstanceOf[Float] || ! java.lang.Float.isNaN(o2.asInstanceOf[Float])) {
              return false
            }
          case d1: Double if java.lang.Double.isNaN(d1) =>
            if (!o2.isInstanceOf[Double] || ! java.lang.Double.isNaN(o2.asInstanceOf[Double])) {
              return false
            }
          case d1: java.math.BigDecimal if o2.isInstanceOf[java.math.BigDecimal] =>
            if (d1.compareTo(o2.asInstanceOf[java.math.BigDecimal]) != 0) {
              return false
            }
          case _ => if (o1 != o2) {
            return false
          }
        }
      }
      i += 1
    }
    true
  }

  override def hashCode: Int = {
    // Using Scala's Seq hash code implementation.
    var n = 0
    var h = MurmurHash3.seqSeed
    val len = length
    while (n < len) {
      h = MurmurHash3.mix(h, apply(n).##)
      n += 1
    }
    MurmurHash3.finalizeHash(h, n)
  }

  /* ---------------------- utility methods for Scala ---------------------- */

  /**
   * Return a Scala Seq representing the row. Elements are placed in the same order in the Seq.
    * 返回表示行的Scala Seq,元素在Seq中以相同的顺序放置
   */
  def toSeq: Seq[Any] = {
    val n = length
    val values = new Array[Any](n)
    var i = 0
    while (i < n) {
      values.update(i, get(i))
      i += 1
    }
    values.toSeq
  }

  /** Displays all elements of this sequence in a string (without a separator).
    * 以字符串形式显示此序列的所有元素(不带分隔符)*/
  def mkString: String = toSeq.mkString

  /** Displays all elements of this sequence in a string using a separator string.
    * 使用分隔符字符串在字符串中显示此序列的所有元素*/
  def mkString(sep: String): String = toSeq.mkString(sep)

  /**
   * Displays all elements of this traversable or iterator in a string using
   * start, end, and separator strings.
    * 使用start,end和separator字符串在字符串中显示此可遍历或迭代器的所有元素
   */
  def mkString(start: String, sep: String, end: String): String = toSeq.mkString(start, sep, end)
}
