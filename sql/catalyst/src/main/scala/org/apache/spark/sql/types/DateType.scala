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

import scala.math.Ordering
import scala.reflect.runtime.universe.typeTag

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.ScalaReflectionLock


/**
 * :: DeveloperApi ::
 * A date type, supporting "0001-01-01" through "9999-12-31".
 *
 * Please use the singleton [[DataTypes.DateType]].
 *
 * Internally, this is represented as the number of days from epoch (1970-01-01 00:00:00 UTC).
  * 在内部,这表示为epoch(1970-01-01 00:00:00 UTC)的天数。
 */
@DeveloperApi
class DateType private() extends AtomicType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "DateType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  //伴随对象和此类是分开的,因此伴随对象也是此类型的子类,
  //否则,伴随对象在字节代码中的类型为“DateType $”。 使用私有构造函数定义,因此伴随对象是唯一可能的实例化
  private[sql] type InternalType = Int

  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[InternalType] }

  private[sql] val ordering = implicitly[Ordering[InternalType]]

  /**
   * The default size of a value of the DateType is 4 bytes.
    * DateType值的默认大小为4个字节
   */
  override def defaultSize: Int = 4

  private[spark] override def asNullable: DateType = this
}


case object DateType extends DateType
