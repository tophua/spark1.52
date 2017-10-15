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

package org.apache.spark.sql.types

/**
 * Utility functions for working with DataTypes in tests.
  * 在测试中使用数据类型的实用功能
 */
object DataTypeTestUtils {

  /**
   * Instances of all [[IntegralType]]s.
   */
  val integralType: Set[IntegralType] = Set(
    ByteType, ShortType, IntegerType, LongType
  )

  /**
   * Instances of all [[FractionalType]]s, including both fixed- and unlimited-precision
   * decimal types.
    * 所有[[FractionalType]]的实例,包括固定和无限精度的十进制类型
   */
  val fractionalTypes: Set[FractionalType] = Set(
    DecimalType.USER_DEFAULT,
    DecimalType(20, 5),
    DecimalType.SYSTEM_DEFAULT,
    DoubleType,
    FloatType
  )

  /**
   * Instances of all [[NumericType]]s.
   */
  val numericTypes: Set[NumericType] = integralType ++ fractionalTypes

  // TODO: remove this once we find out how to handle decimal properly in property check
  val numericTypeWithoutDecimal: Set[DataType] = integralType ++ Set(DoubleType, FloatType)

  /**
   * Instances of all [[NumericType]]s and [[CalendarIntervalType]]
    * 所有[[NumericType]]和[[CalendarIntervalType]]的实例
   */
  val numericAndInterval: Set[DataType] = numericTypeWithoutDecimal + CalendarIntervalType

  /**
   * All the types that support ordering
    * 支持订购的所有类型
   */
  val ordered: Set[DataType] =
    numericTypeWithoutDecimal + BooleanType + TimestampType + DateType + StringType + BinaryType

  /**
   * All the types that we can use in a property check
    * 我们可以在属性检查中使用的所有类型
   */
  val propertyCheckSupported: Set[DataType] = ordered

  /**
   * Instances of all [[AtomicType]]s.
   */
  val atomicTypes: Set[DataType] = numericTypes ++ Set(
    BinaryType,
    BooleanType,
    DateType,
    StringType,
    TimestampType
  )

  /**
   * Instances of [[ArrayType]] for all [[AtomicType]]s. Arrays of these types may contain null.
    * [[ArrayType]]的所有[[AtomicType]]的实例,这些类型的数组可能包含null
   */
  val atomicArrayTypes: Set[ArrayType] = atomicTypes.map(ArrayType(_, containsNull = true))
}
