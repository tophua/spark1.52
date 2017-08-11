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

package org.apache.spark.util

import java.io.NotSerializableException

import scala.collection.mutable

import org.scalatest.{BeforeAndAfterAll, PrivateMethodTester}

import org.apache.spark.{SparkContext, SparkException, SparkFunSuite}
import org.apache.spark.serializer.SerializerInstance

/**
 * Another test suite for the closure cleaner that is finer-grained.
  * 封闭清理的另一个测试套件更细腻
 * For tests involving end-to-end Spark jobs, see {{ClosureCleanerSuite}}.
  * 对于涉及端到端Spark作业的测试,请参阅{{ClosureCleanerSuite}}
 */
class ClosureCleanerSuite2 extends SparkFunSuite with BeforeAndAfterAll with PrivateMethodTester {

  // Start a SparkContext so that the closure serializer is accessible
  // We do not actually use this explicitly otherwise
  //启动一个SparkContext,以便封装序列化程序可以访问我们实际上并没有明确使用它
  private var sc: SparkContext = null
  private var closureSerializer: SerializerInstance = null

  override def beforeAll(): Unit = {
    sc = new SparkContext("local", "test")
    closureSerializer = sc.env.closureSerializer.newInstance()
  }

  override def afterAll(): Unit = {
    sc.stop()
    sc = null
    closureSerializer = null
  }

  // Some fields and methods to reference in inner closures later
  //稍后在内部关闭中引用的一些字段和方法
  private val someSerializableValue = 1
  private val someNonSerializableValue = new NonSerializable
  private def someSerializableMethod() = 1
  private def someNonSerializableMethod() = new NonSerializable

  /** Assert that the given closure is serializable (or not).
    * 断言给定的闭包是可序列化的(或不是)*/
  private def assertSerializable(closure: AnyRef, serializable: Boolean): Unit = {
    if (serializable) {
      closureSerializer.serialize(closure)
    } else {
      intercept[NotSerializableException] {
        closureSerializer.serialize(closure)
      }
    }
  }

  /**
   * Helper method for testing whether closure cleaning works as expected.
    * 帮助者测试关闭清理是否按预期工作
   * This cleans the given closure twice, with and without transitive cleaning.
    * 这样可以清洗两次给定的封盖,并进行和不进行过滤清洁
   *
   * @param closure closure to test cleaning with 关闭以测试清理
   * @param serializableBefore if true, verify that the closure is serializable
   *                           before cleaning, otherwise assert that it is not
    *                           如果是,请在清理之前验证封闭是否可序列化,否则声明不是
   * @param serializableAfter if true, assert that the closure is serializable
   *                          after cleaning otherwise assert that it is not
    *                          如果为真,则断言闭包可以在清除之后进行序列化,否则声明它不是
   */
  private def verifyCleaning(
      closure: AnyRef,
      serializableBefore: Boolean,
      serializableAfter: Boolean): Unit = {
    verifyCleaning(closure, serializableBefore, serializableAfter, transitive = true)
    verifyCleaning(closure, serializableBefore, serializableAfter, transitive = false)
  }

  /** Helper method for testing whether closure cleaning works as expected.
    * 帮助者测试关闭清洁是否按预期工作*/
  private def verifyCleaning(
      closure: AnyRef,
      serializableBefore: Boolean,
      serializableAfter: Boolean,
      transitive: Boolean): Unit = {
    assertSerializable(closure, serializableBefore)
    // If the resulting closure is not serializable even after
    // cleaning, we expect ClosureCleaner to throw a SparkException
    //如果生成的闭包即使在清理后也不是可序列化的,我们期望ClosureCleaner抛出一个SparkException
    if (serializableAfter) {
      ClosureCleaner.clean(closure, checkSerializable = true, transitive)
    } else {
      intercept[SparkException] {
        ClosureCleaner.clean(closure, checkSerializable = true, transitive)
      }
    }
    assertSerializable(closure, serializableAfter)
  }

  /**
   * Return the fields accessed by the given closure by class.
    * 通过类返回由给定闭包访问的字段
   * This also optionally finds the fields transitively referenced through methods invocations.
    * 这也可以通过方法调用来过渡地引用字段
   */
  private def findAccessedFields(
      closure: AnyRef,
      outerClasses: Seq[Class[_]],
      findTransitively: Boolean): Map[Class[_], Set[String]] = {
    val fields = new mutable.HashMap[Class[_], mutable.Set[String]]
    outerClasses.foreach { c => fields(c) = new mutable.HashSet[String] }
    ClosureCleaner.getClassReader(closure.getClass)
      .accept(new FieldAccessFinder(fields, findTransitively), 0)
    fields.mapValues(_.toSet).toMap
  }

  // Accessors for private methods 私人方式的访问者
  private val _isClosure = PrivateMethod[Boolean]('isClosure)
  private val _getInnerClosureClasses = PrivateMethod[List[Class[_]]]('getInnerClosureClasses)
  private val _getOuterClassesAndObjects =
    PrivateMethod[(List[Class[_]], List[AnyRef])]('getOuterClassesAndObjects)

  private def isClosure(obj: AnyRef): Boolean = {
    ClosureCleaner invokePrivate _isClosure(obj)
  }

  private def getInnerClosureClasses(closure: AnyRef): List[Class[_]] = {
    ClosureCleaner invokePrivate _getInnerClosureClasses(closure)
  }

  private def getOuterClassesAndObjects(closure: AnyRef): (List[Class[_]], List[AnyRef]) = {
    ClosureCleaner invokePrivate _getOuterClassesAndObjects(closure)
  }
//获取内部闭包类
  test("get inner closure classes") {
    val closure1 = () => 1
    val closure2 = () => { () => 1 }
    val closure3 = (i: Int) => {
      (1 to i).map { x => x + 1 }.filter { x => x > 5 }
    }
    val closure4 = (j: Int) => {
      (1 to j).flatMap { x =>
        (1 to x).flatMap { y =>
          (1 to y).map { z => z + 1 }
        }
      }
    }
    val inner1 = getInnerClosureClasses(closure1)
    val inner2 = getInnerClosureClasses(closure2)
    val inner3 = getInnerClosureClasses(closure3)
    val inner4 = getInnerClosureClasses(closure4)
    assert(inner1.isEmpty)
    assert(inner2.size === 1)
    assert(inner3.size === 2)
    assert(inner4.size === 3)
    assert(inner2.forall(isClosure))
    assert(inner3.forall(isClosure))
    assert(inner4.forall(isClosure))
  }
  //获取外部类和对象
  test("get outer classes and objects") {
    val localValue = someSerializableValue
    val closure1 = () => 1
    val closure2 = () => localValue
    val closure3 = () => someSerializableValue
    val closure4 = () => someSerializableMethod()

    val (outerClasses1, outerObjects1) = getOuterClassesAndObjects(closure1)
    val (outerClasses2, outerObjects2) = getOuterClassesAndObjects(closure2)
    val (outerClasses3, outerObjects3) = getOuterClassesAndObjects(closure3)
    val (outerClasses4, outerObjects4) = getOuterClassesAndObjects(closure4)

    // The classes and objects should have the same size
    //类和对象应具有相同的大小
    assert(outerClasses1.size === outerObjects1.size)
    assert(outerClasses2.size === outerObjects2.size)
    assert(outerClasses3.size === outerObjects3.size)
    assert(outerClasses4.size === outerObjects4.size)

    // These do not have $outer pointers because they reference only local variables
    //这些没有$外部指针，因为它们仅引用局部变量
    assert(outerClasses1.isEmpty)
    assert(outerClasses2.isEmpty)

    // These closures do have $outer pointers because they ultimately reference `this`
    // The first $outer pointer refers to the closure defines this test (see FunSuite#test)
    // The second $outer pointer refers to ClosureCleanerSuite2
    assert(outerClasses3.size === 2)
    assert(outerClasses4.size === 2)
    assert(isClosure(outerClasses3(0)))
    assert(isClosure(outerClasses4(0)))
    assert(outerClasses3(0) === outerClasses4(0)) // part of the same "FunSuite#test" scope
    assert(outerClasses3(1) === this.getClass)
    assert(outerClasses4(1) === this.getClass)
    assert(outerObjects3(1) === this)
    assert(outerObjects4(1) === this)
  }
//获取外部类和嵌套对象
  test("get outer classes and objects with nesting") {
    val localValue = someSerializableValue

    val test1 = () => {
      val x = 1
      val closure1 = () => 1
      val closure2 = () => x
      val (outerClasses1, outerObjects1) = getOuterClassesAndObjects(closure1)
      val (outerClasses2, outerObjects2) = getOuterClassesAndObjects(closure2)
      assert(outerClasses1.size === outerObjects1.size)
      assert(outerClasses2.size === outerObjects2.size)
      // These inner closures only reference local variables, and so do not have $outer pointers
      //这些内部闭包仅引用局部变量，因此没有$外部指针
      assert(outerClasses1.isEmpty)
      assert(outerClasses2.isEmpty)
    }

    val test2 = () => {
      def y = 1
      val closure1 = () => 1
      val closure2 = () => y
      val closure3 = () => localValue
      val (outerClasses1, outerObjects1) = getOuterClassesAndObjects(closure1)
      val (outerClasses2, outerObjects2) = getOuterClassesAndObjects(closure2)
      val (outerClasses3, outerObjects3) = getOuterClassesAndObjects(closure3)
      assert(outerClasses1.size === outerObjects1.size)
      assert(outerClasses2.size === outerObjects2.size)
      assert(outerClasses3.size === outerObjects3.size)
      // Same as above, this closure only references local variables
      //与上述相同，此关闭仅引用局部变量
      assert(outerClasses1.isEmpty)
      // This closure references the "test2" scope because it needs to find the method `y`
      // Scope hierarchy: "test2" < "FunSuite#test" < ClosureCleanerSuite2
      //这个闭包引用“test2”范围，因为它需要找到方法`y`范围层级：“test2”<“FunSuite＃test”<ClosureCleanerSuite2
      assert(outerClasses2.size === 3)
      // This closure references the "test2" scope because it needs to find the `localValue`
      // defined outside of this scope
      //此闭包引用“test2”范围,因为它需要找到在此范围之外定义的“localValue”
      assert(outerClasses3.size === 3)
      assert(isClosure(outerClasses2(0)))
      assert(isClosure(outerClasses3(0)))
      assert(isClosure(outerClasses2(1)))
      assert(isClosure(outerClasses3(1)))
      //部分相同的“test2”范围
      assert(outerClasses2(0) === outerClasses3(0)) // part of the same "test2" scope
      //部分相同的“FunSuite＃test”范围
      assert(outerClasses2(1) === outerClasses3(1)) // part of the same "FunSuite#test" scope
      assert(outerClasses2(2) === this.getClass)
      assert(outerClasses3(2) === this.getClass)
      assert(outerObjects2(2) === this)
      assert(outerObjects3(2) === this)
    }

    test1()
    test2()
  }
  //找到访问的领域
  test("find accessed fields") {
    val localValue = someSerializableValue
    val closure1 = () => 1
    val closure2 = () => localValue
    val closure3 = () => someSerializableValue
    val (outerClasses1, _) = getOuterClassesAndObjects(closure1)
    val (outerClasses2, _) = getOuterClassesAndObjects(closure2)
    val (outerClasses3, _) = getOuterClassesAndObjects(closure3)

    val fields1 = findAccessedFields(closure1, outerClasses1, findTransitively = false)
    val fields2 = findAccessedFields(closure2, outerClasses2, findTransitively = false)
    val fields3 = findAccessedFields(closure3, outerClasses3, findTransitively = false)
    assert(fields1.isEmpty)
    assert(fields2.isEmpty)
    assert(fields3.size === 2)
    // This corresponds to the "FunSuite#test" closure. This is empty because the
    // `someSerializableValue` belongs to its parent (i.e. ClosureCleanerSuite2).
    //这对应于“FunSuite＃test”关闭。这是空的,因为`someSerializableValue`属于它的父(即ClosureCleanerSuite2)
    assert(fields3(outerClasses3(0)).isEmpty)
    // This corresponds to the ClosureCleanerSuite2. This is also empty, however,
    // because accessing a `ClosureCleanerSuite2#someSerializableValue` actually involves a
    // method call. Since we do not find fields transitively, we will not recursively trace
    // through the fields referenced by this method.
    assert(fields3(outerClasses3(1)).isEmpty)

    val fields1t = findAccessedFields(closure1, outerClasses1, findTransitively = true)
    val fields2t = findAccessedFields(closure2, outerClasses2, findTransitively = true)
    val fields3t = findAccessedFields(closure3, outerClasses3, findTransitively = true)
    assert(fields1t.isEmpty)
    assert(fields2t.isEmpty)
    assert(fields3t.size === 2)
    // Because we find fields transitively now, we are able to detect that we need the
    // $outer pointer to get the field from the ClosureCleanerSuite2
    //因为现在我们发现了现场,我们可以检测到我们需要$ outer指针来从ClosureCleanerSuite2
    assert(fields3t(outerClasses3(0)).size === 1)
    assert(fields3t(outerClasses3(0)).head === "$outer")
    assert(fields3t(outerClasses3(1)).size === 1)
    assert(fields3t(outerClasses3(1)).head.contains("someSerializableValue"))
  }

  test("find accessed fields with nesting") {
    val localValue = someSerializableValue

    val test1 = () => {
      def a = localValue + 1
      val closure1 = () => 1
      val closure2 = () => a
      val closure3 = () => localValue
      val closure4 = () => someSerializableValue
      val (outerClasses1, _) = getOuterClassesAndObjects(closure1)
      val (outerClasses2, _) = getOuterClassesAndObjects(closure2)
      val (outerClasses3, _) = getOuterClassesAndObjects(closure3)
      val (outerClasses4, _) = getOuterClassesAndObjects(closure4)

      // First, find only fields accessed directly, not transitively, by these closures
      //首先,只能通过这些关闭直接访问字段,而不是传递性地访问
      val fields1 = findAccessedFields(closure1, outerClasses1, findTransitively = false)
      val fields2 = findAccessedFields(closure2, outerClasses2, findTransitively = false)
      val fields3 = findAccessedFields(closure3, outerClasses3, findTransitively = false)
      val fields4 = findAccessedFields(closure4, outerClasses4, findTransitively = false)
      assert(fields1.isEmpty)
      // Note that the size here represents the number of outer classes, not the number of fields
      // "test1" < parameter of "FunSuite#test" < ClosureCleanerSuite2
      //请注意，此处的大小表示外部类的数量，而不是字段数“test1”<“FunSuite＃test”的参数<ClosureCleanerSuite2
      assert(fields2.size === 3)
      // Since we do not find fields transitively here, we do not look into what `def a` references
      //既然我们在这里没有找到领域，所以我们不去研究什么`def a`参考
      //这对应于“test1”范围
      assert(fields2(outerClasses2(0)).isEmpty) // This corresponds to the "test1" scope
      assert(fields2(outerClasses2(1)).isEmpty) // This corresponds to the "FunSuite#test" scope
      assert(fields2(outerClasses2(2)).isEmpty) // This corresponds to the ClosureCleanerSuite2
      assert(fields3.size === 3)
      // Note that `localValue` is a field of the "test1" scope because `def a` references it,
      //请注意,`localValue`是“test1”范围的一个字段，因为`def a`引用它，
      // but NOT a field of the "FunSuite#test" scope because it is only a local variable there
      //但不是“FunSuite＃test”范围的一个字段,因为它只是一个局部变量
      assert(fields3(outerClasses3(0)).size === 1)
      assert(fields3(outerClasses3(0)).head.contains("localValue"))
      assert(fields3(outerClasses3(1)).isEmpty)
      assert(fields3(outerClasses3(2)).isEmpty)
      assert(fields4.size === 3)
      // Because `val someSerializableValue` is an instance variable, even an explicit reference
      // here actually involves a method call to access the underlying value of the variable.
      // Because we are not finding fields transitively here, we do not consider the fields
      // accessed by this "method" (i.e. the val's accessor).
      assert(fields4(outerClasses4(0)).isEmpty)
      assert(fields4(outerClasses4(1)).isEmpty)
      assert(fields4(outerClasses4(2)).isEmpty)

      // Now do the same, but find fields that the closures transitively reference
      //现在做同样的事情，但找到关闭的领域过渡性地参考
      val fields1t = findAccessedFields(closure1, outerClasses1, findTransitively = true)
      val fields2t = findAccessedFields(closure2, outerClasses2, findTransitively = true)
      val fields3t = findAccessedFields(closure3, outerClasses3, findTransitively = true)
      val fields4t = findAccessedFields(closure4, outerClasses4, findTransitively = true)
      assert(fields1t.isEmpty)
      assert(fields2t.size === 3)
      assert(fields2t(outerClasses2(0)).size === 1) // `def a` references `localValue`
      assert(fields2t(outerClasses2(0)).head.contains("localValue"))
      assert(fields2t(outerClasses2(1)).isEmpty)
      assert(fields2t(outerClasses2(2)).isEmpty)
      assert(fields3t.size === 3)
      assert(fields3t(outerClasses3(0)).size === 1) // as before
      assert(fields3t(outerClasses3(0)).head.contains("localValue"))
      assert(fields3t(outerClasses3(1)).isEmpty)
      assert(fields3t(outerClasses3(2)).isEmpty)
      assert(fields4t.size === 3)
      // Through a series of method calls, we are able to detect that we ultimately access
      // ClosureCleanerSuite2's field `someSerializableValue`. Along the way, we also accessed
      // a few $outer parent pointers to get to the outermost object.
      assert(fields4t(outerClasses4(0)) === Set("$outer"))
      assert(fields4t(outerClasses4(1)) === Set("$outer"))
      assert(fields4t(outerClasses4(2)).size === 1)
      assert(fields4t(outerClasses4(2)).head.contains("someSerializableValue"))
    }

    test1()
  }

  test("clean basic serializable closures") {
    val localValue = someSerializableValue
    val closure1 = () => 1
    val closure2 = () => Array[String]("a", "b", "c")
    val closure3 = (s: String, arr: Array[Long]) => s + arr.mkString(", ")
    val closure4 = () => localValue
    val closure5 = () => new NonSerializable(5) // we're just serializing the class information
    val closure1r = closure1()
    val closure2r = closure2()
    val closure3r = closure3("g", Array(1, 5, 8))
    val closure4r = closure4()
    val closure5r = closure5()

    verifyCleaning(closure1, serializableBefore = true, serializableAfter = true)
    verifyCleaning(closure2, serializableBefore = true, serializableAfter = true)
    verifyCleaning(closure3, serializableBefore = true, serializableAfter = true)
    verifyCleaning(closure4, serializableBefore = true, serializableAfter = true)
    verifyCleaning(closure5, serializableBefore = true, serializableAfter = true)

    // Verify that closures can still be invoked and the result still the same
    assert(closure1() === closure1r)
    assert(closure2() === closure2r)
    assert(closure3("g", Array(1, 5, 8)) === closure3r)
    assert(closure4() === closure4r)
    assert(closure5() === closure5r)
  }

  test("clean basic non-serializable closures") {
    val closure1 = () => this // ClosureCleanerSuite2 is not serializable
    val closure5 = () => someSerializableValue
    val closure3 = () => someSerializableMethod()
    val closure4 = () => someNonSerializableValue
    val closure2 = () => someNonSerializableMethod()

    // These are not cleanable because they ultimately reference the ClosureCleanerSuite2
    verifyCleaning(closure1, serializableBefore = false, serializableAfter = false)
    verifyCleaning(closure2, serializableBefore = false, serializableAfter = false)
    verifyCleaning(closure3, serializableBefore = false, serializableAfter = false)
    verifyCleaning(closure4, serializableBefore = false, serializableAfter = false)
    verifyCleaning(closure5, serializableBefore = false, serializableAfter = false)
  }

  test("clean basic nested serializable closures") {
    val localValue = someSerializableValue
    val closure1 = (i: Int) => {
      (1 to i).map { x => x + localValue } // 1 level of nesting
    }
    val closure2 = (j: Int) => {
      (1 to j).flatMap { x =>
        (1 to x).map { y => y + localValue } // 2 levels
      }
    }
    val closure3 = (k: Int, l: Int, m: Int) => {
      (1 to k).flatMap(closure2) ++ // 4 levels
      (1 to l).flatMap(closure1) ++ // 3 levels
      (1 to m).map { x => x + 1 } // 2 levels
    }
    val closure1r = closure1(1)
    val closure2r = closure2(2)
    val closure3r = closure3(3, 4, 5)

    verifyCleaning(closure1, serializableBefore = true, serializableAfter = true)
    verifyCleaning(closure2, serializableBefore = true, serializableAfter = true)
    verifyCleaning(closure3, serializableBefore = true, serializableAfter = true)

    // Verify that closures can still be invoked and the result still the same
    assert(closure1(1) === closure1r)
    assert(closure2(2) === closure2r)
    assert(closure3(3, 4, 5) === closure3r)
  }

  test("clean basic nested non-serializable closures") {
    def localSerializableMethod(): Int = someSerializableValue
    val localNonSerializableValue = someNonSerializableValue
    // These closures ultimately reference the ClosureCleanerSuite2
    // Note that even accessing `val` that is an instance variable involves a method call
    val closure1 = (i: Int) => { (1 to i).map { x => x + someSerializableValue } }
    val closure2 = (j: Int) => { (1 to j).map { x => x + someSerializableMethod() } }
    val closure4 = (k: Int) => { (1 to k).map { x => x + localSerializableMethod() } }
    // This closure references a local non-serializable value
    val closure3 = (l: Int) => { (1 to l).map { x => localNonSerializableValue } }
    // This is non-serializable no matter how many levels we nest it
    val closure5 = (m: Int) => {
      (1 to m).foreach { x =>
        (1 to x).foreach { y =>
          (1 to y).foreach { z =>
            someSerializableValue
          }
        }
      }
    }

    verifyCleaning(closure1, serializableBefore = false, serializableAfter = false)
    verifyCleaning(closure2, serializableBefore = false, serializableAfter = false)
    verifyCleaning(closure3, serializableBefore = false, serializableAfter = false)
    verifyCleaning(closure4, serializableBefore = false, serializableAfter = false)
    verifyCleaning(closure5, serializableBefore = false, serializableAfter = false)
  }

  test("clean complicated nested serializable closures") {
    val localValue = someSerializableValue

    // Here we assume that if the outer closure is serializable,
    // then all inner closures must also be serializable

    // Reference local fields from all levels
    val closure1 = (i: Int) => {
      val a = 1
      (1 to i).flatMap { x =>
        val b = a + 1
        (1 to x).map { y =>
          y + a + b + localValue
        }
      }
    }

    // Reference local fields and methods from all levels within the outermost closure
    val closure2 = (i: Int) => {
      val a1 = 1
      def a2 = 2
      (1 to i).flatMap { x =>
        val b1 = a1 + 1
        def b2 = a2 + 1
        (1 to x).map { y =>
          // If this references a method outside the outermost closure, then it will try to pull
          // in the ClosureCleanerSuite2. This is why `localValue` here must be a local `val`.
          y + a1 + a2 + b1 + b2 + localValue
        }
      }
    }

    val closure1r = closure1(1)
    val closure2r = closure2(2)
    verifyCleaning(closure1, serializableBefore = true, serializableAfter = true)
    verifyCleaning(closure2, serializableBefore = true, serializableAfter = true)
    assert(closure1(1) == closure1r)
    assert(closure2(2) == closure2r)
  }

  test("clean complicated nested non-serializable closures") {
    val localValue = someSerializableValue

    // Note that we are not interested in cleaning the outer closures here (they are not cleanable)
    // The only reason why they exist is to nest the inner closures

    val test1 = () => {
      val a = localValue
      val b = sc
      val inner1 = (x: Int) => x + a + b.hashCode()
      val inner2 = (x: Int) => x + a

      // This closure explicitly references a non-serializable field
      // There is no way to clean it
      verifyCleaning(inner1, serializableBefore = false, serializableAfter = false)

      // This closure is serializable to begin with since it does not need a pointer to
      // the outer closure (it only references local variables)
      verifyCleaning(inner2, serializableBefore = true, serializableAfter = true)
    }

    // Same as above, but the `val a` becomes `def a`
    // The difference here is that all inner closures now have pointers to the outer closure
    val test2 = () => {
      def a = localValue
      val b = sc
      val inner1 = (x: Int) => x + a + b.hashCode()
      val inner2 = (x: Int) => x + a

      // As before, this closure is neither serializable nor cleanable
      verifyCleaning(inner1, serializableBefore = false, serializableAfter = false)

      // This closure is no longer serializable because it now has a pointer to the outer closure,
      // which is itself not serializable because it has a pointer to the ClosureCleanerSuite2.
      // If we do not clean transitively, we will not null out this indirect reference.
      verifyCleaning(
        inner2, serializableBefore = false, serializableAfter = false, transitive = false)

      // If we clean transitively, we will find that method `a` does not actually reference the
      // outer closure's parent (i.e. the ClosureCleanerSuite), so we can additionally null out
      // the outer closure's parent pointer. This will make `inner2` serializable.
      verifyCleaning(
        inner2, serializableBefore = false, serializableAfter = true, transitive = true)
    }

    // Same as above, but with more levels of nesting
    val test3 = () => { () => test1() }
    val test4 = () => { () => test2() }
    val test5 = () => { () => { () => test3() } }
    val test6 = () => { () => { () => test4() } }

    test1()
    test2()
    test3()()
    test4()()
    test5()()()
    test6()()()
  }

}
