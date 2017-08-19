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

package org.apache.spark.serializer

import java.io._

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite


class SerializationDebuggerSuite extends SparkFunSuite with BeforeAndAfterEach {

  import SerializationDebugger.find

  override def beforeEach(): Unit = {
    SerializationDebugger.enableDebugging = true
  }

  test("primitives, strings, and nulls") {//原始,字符串,和空
    assert(find(1) === List.empty)
    assert(find(1L) === List.empty)
    assert(find(1.toShort) === List.empty)
    assert(find(1.0) === List.empty)
    assert(find("1") === List.empty)
    assert(find(null) === List.empty)
  }

  test("primitive arrays") {//原始的数组
    assert(find(Array[Int](1, 2)) === List.empty)
    assert(find(Array[Long](1, 2)) === List.empty)
  }

  test("non-primitive arrays") {//非原始数组
    assert(find(Array("aa", "bb")) === List.empty)
    assert(find(Array(new SerializableClass1)) === List.empty)
  }

  test("serializable object") {//可序列化的对象
    assert(find(new Foo(1, "b", 'c', 'd', null, null, null)) === List.empty)
  }

  test("nested arrays") {//嵌套的数组
    val foo1 = new Foo(1, "b", 'c', 'd', null, null, null)
    val foo2 = new Foo(1, "b", 'c', 'd', null, Array(foo1), null)
    assert(find(new Foo(1, "b", 'c', 'd', null, Array(foo2), null)) === List.empty)
  }

  test("nested objects") {//嵌套对象
    val foo1 = new Foo(1, "b", 'c', 'd', null, null, null)
    val foo2 = new Foo(1, "b", 'c', 'd', null, null, foo1)
    assert(find(new Foo(1, "b", 'c', 'd', null, null, foo2)) === List.empty)
  }

  test("cycles (should not loop forever)") {//周期,不应该永远循环
    val foo1 = new Foo(1, "b", 'c', 'd', null, null, null)
    foo1.g = foo1
    assert(find(new Foo(1, "b", 'c', 'd', null, null, foo1)) === List.empty)
  }

  test("root object not serializable") {//根对象不可序列化
    val s = find(new NotSerializable)
    assert(s.size === 1)
    assert(s.head.contains("NotSerializable"))
  }

  test("array containing not serializable element") {//数组包含不可序列化元素
    val s = find(new SerializableArray(Array(new NotSerializable)))
    assert(s.size === 5)
    assert(s(0).contains("NotSerializable"))
    assert(s(1).contains("element of array"))
    assert(s(2).contains("array"))
    assert(s(3).contains("arrayField"))
    assert(s(4).contains("SerializableArray"))
  }

  test("object containing not serializable field") {//对象包含不可序列化字段
    val s = find(new SerializableClass2(new NotSerializable))
    assert(s.size === 3)
    assert(s(0).contains("NotSerializable"))
    assert(s(1).contains("objectField"))
    assert(s(2).contains("SerializableClass2"))
  }
  //外部的类写不可序列化的对象
  test("externalizable class writing out not serializable object") {
    val s = find(new ExternalizableClass(new SerializableClass2(new NotSerializable)))
    assert(s.size === 5)
    assert(s(0).contains("NotSerializable"))
    assert(s(1).contains("objectField"))
    assert(s(2).contains("SerializableClass2"))
    assert(s(3).contains("writeExternal"))
    assert(s(4).contains("ExternalizableClass"))
  }
  //外部的类写序列化对象
  test("externalizable class writing out serializable objects") {
    assert(find(new ExternalizableClass(new SerializableClass1)).isEmpty)
  }
  //对象包含writereplace()返回不可序列化的对象
  test("object containing writeReplace() which returns not serializable object") {
    val s = find(new SerializableClassWithWriteReplace(new NotSerializable))
    assert(s.size === 3)
    assert(s(0).contains("NotSerializable"))
    assert(s(1).contains("writeReplace"))
    assert(s(2).contains("SerializableClassWithWriteReplace"))
  }
  //对象包含writereplace()返回序列化对象
  test("object containing writeReplace() which returns serializable object") {
    assert(find(new SerializableClassWithWriteReplace(new SerializableClass1)).isEmpty)
  }
  //对象包含writereplace()返回不可序列化的对象字段
    test("object containing writeObject() and not serializable field") {
    val s = find(new SerializableClassWithWriteObject(new NotSerializable))
    assert(s.size === 3)
    assert(s(0).contains("NotSerializable"))
    assert(s(1).contains("writeObject data"))
    assert(s(2).contains("SerializableClassWithWriteObject"))
  }
  //对象包含writereplace()返回可序列化的对象字段
  test("object containing writeObject() and serializable field") {
    assert(find(new SerializableClassWithWriteObject(new SerializableClass1)).isEmpty)
  }

  test("object of serializable subclass with more fields than superclass (SPARK-7180)") {
    // This should not throw ArrayOutOfBoundsException
    find(new SerializableSubclass(new SerializableClass1))
  }

  test("crazy nested objects") {//疯狂的嵌套对象

    def findAndAssert(shouldSerialize: Boolean, obj: Any): Unit = {
      val s = find(obj)
      if (shouldSerialize) {
        assert(s.isEmpty)
      } else {
        assert(s.nonEmpty)
        assert(s.head.contains("NotSerializable"))
      }
    }

    findAndAssert(false,
      new SerializableClassWithWriteReplace(new ExternalizableClass(new SerializableSubclass(
        new SerializableArray(
          Array(new SerializableClass1, new SerializableClass2(new NotSerializable))
        )
      )))
    )

    findAndAssert(true,
      new SerializableClassWithWriteReplace(new ExternalizableClass(new SerializableSubclass(
        new SerializableArray(
          Array(new SerializableClass1, new SerializableClass2(new SerializableClass1))
        )
      )))
    )
  }

  test("improveException") {//提高异常
    val e = SerializationDebugger.improveException(
      new SerializableClass2(new NotSerializable), new NotSerializableException("someClass"))
    assert(e.getMessage.contains("someClass"))  // original exception message should be present
    assert(e.getMessage.contains("SerializableClass2"))  // found debug trace should be present
  }

  test("improveException with error in debugger") {//在调试器中提高异常与错误
    // Object that throws exception in the SerializationDebugger
    val o = new SerializableClass1 {
      private def writeReplace(): Object = {
        throw new Exception()
      }
    }
    //应该尝试调试此对象
    withClue("requirement: SerializationDebugger should fail trying debug this object") {
      intercept[Exception] {
        SerializationDebugger.find(o)
      }
    }

    val originalException = new NotSerializableException("someClass")
    // verify thaht original exception is returned on failure
    //验证失败时是否返回原始异常
    assert(SerializationDebugger.improveException(o, originalException).eq(originalException))
  }
}


class SerializableClass1 extends Serializable


class SerializableClass2(val objectField: Object) extends Serializable


class SerializableArray(val arrayField: Array[Object]) extends Serializable


class SerializableSubclass(val objectField: Object) extends SerializableClass1


class SerializableClassWithWriteObject(val objectField: Object) extends Serializable {
  val serializableObjectField = new SerializableClass1

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = {
    oos.defaultWriteObject()
  }
}


class SerializableClassWithWriteReplace(@transient replacementFieldObject: Object)
  extends Serializable {
  private def writeReplace(): Object = {
    replacementFieldObject
  }
}


class ExternalizableClass(objectField: Object) extends java.io.Externalizable {
  val serializableObjectField = new SerializableClass1

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeInt(1)
    out.writeObject(serializableObjectField)
    out.writeObject(objectField)
  }

  override def readExternal(in: ObjectInput): Unit = {}
}


class Foo(
    a: Int,
    b: String,
    c: Char,
    d: Byte,
    e: Array[Int],
    f: Array[Object],
    var g: Foo) extends Serializable


class NotSerializable
