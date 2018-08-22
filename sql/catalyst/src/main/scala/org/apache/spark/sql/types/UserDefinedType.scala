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

import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._

import org.apache.spark.annotation.DeveloperApi

/**
 * ::DeveloperApi::
 * The data type for User Defined Types (UDTs).
  * 用户定义类型（UDT）的数据类型
 *
 * This interface allows a user to make their own classes more interoperable with SparkSQL;
  * 该接口允许用户使自己的类与SparkSQL更具互操作性;
 * e.g., by creating a [[UserDefinedType]] for a class X, it becomes possible to create
 * a `DataFrame` which has class X in the schema.
 *
 * For SparkSQL to recognize UDTs, the UDT must be annotated with
  * 要使SparkSQL识别UDT,必须使用注释UDT
 * [[SQLUserDefinedType]].
 *
 * The conversion via `serialize` occurs when instantiating a `DataFrame` from another RDD.
 * The conversion via `deserialize` occurs when reading from a `DataFrame`.
  *
  * 当从另一个RDD实例化`DataFrame`时,通过`serialize`进行转换,
  * 当从`DataFrame`读取时,通过`deserialize`进行转换。
 */
@DeveloperApi
abstract class UserDefinedType[UserType] extends DataType with Serializable {

  /** Underlying storage type for this UDT
    * 此UDT的底层存储类型*/
  def sqlType: DataType

  /** Paired Python UDT class, if exists.
    * 配对的Python UDT类,如果存在*/
  def pyUDT: String = null

  /** Serialized Python UDT class, if exists. 序列化的Python UDT类(如果存在)*/
  def serializedPyClass: String = null

  /**
   * Convert the user type to a SQL datum
    * 将用户类型转换为SQL数据
   *
   * TODO: Can we make this take obj: UserType?  The issue is in
   *       CatalystTypeConverters.convertToCatalyst, where we need to convert Any to UserType.
   */
  def serialize(obj: Any): Any

  /** Convert a SQL datum to the user type
    * 将SQL数据转换为用户类型*/
  def deserialize(datum: Any): UserType

  override private[sql] def jsonValue: JValue = {
    ("type" -> "udt") ~
      ("class" -> this.getClass.getName) ~
      ("pyClass" -> pyUDT) ~
      ("sqlType" -> sqlType.jsonValue)
  }

  /**
   * Class object for the UserType
    * UserType的类对象
   */
  def userClass: java.lang.Class[UserType]

  /**
   * The default size of a value of the UserDefinedType is 4096 bytes.
    * serDefinedType的值的默认大小为4096字节
   */
  override def defaultSize: Int = 4096

  /**
   * For UDT, asNullable will not change the nullability of its internal sqlType and just returns
   * itself.
    * 对于UDT,asNullable不会改变其内部sqlType的可为空性,只返回自身
   */
  override private[spark] def asNullable: UserDefinedType[UserType] = this

  override private[sql] def acceptsType(dataType: DataType) =
    this.getClass == dataType.getClass
}

/**
 * ::DeveloperApi::
 * The user defined type in Python.
 *
 * Note: This can only be accessed via Python UDF, or accessed as serialized object.
  * 注意：这只能通过Python UDF访问,或作为序列化对象访问
 */
private[sql] class PythonUserDefinedType(
    val sqlType: DataType,
    override val pyUDT: String,
    override val serializedPyClass: String) extends UserDefinedType[Any] {

  /* The serialization is handled by UDT class in Python
  * 序列化由Python中的UDT类处理*/
  override def serialize(obj: Any): Any = obj
  override def deserialize(datam: Any): Any = datam

  /* There is no Java class for Python UDT
  * Python UDT没有Java类*/
  override def userClass: java.lang.Class[Any] = null

  override private[sql] def jsonValue: JValue = {
    ("type" -> "udt") ~
      ("pyClass" -> pyUDT) ~
      ("serializedClass" -> serializedPyClass) ~
      ("sqlType" -> sqlType.jsonValue)
  }
}
