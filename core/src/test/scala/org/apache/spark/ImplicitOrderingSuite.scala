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

package org.apache.spark

import org.apache.spark.rdd.RDD

class ImplicitOrderingSuite extends SparkFunSuite with LocalSparkContext {
  // Tests that PairRDDFunctions grabs an implicit Ordering in various cases where it should.
  //测试PairRDDFunctions在各种情况下抓取隐式排序
  test("basic inference of Orderings"){
    sc = new SparkContext("local", "test")
    val rdd = sc.parallelize(1 to 10)

    // These RDD methods are in the companion object so that the unserializable ScalaTest Engine
    // won't be reachable from the closure object
      //这些RDD方法在配对对象中，使得不可串行化的ScalaTest引擎将无法从闭包对象中访问
    // Infer orderings after basic maps to particular types
    //在基本映射到特定类型之后进行排序
    val basicMapExpectations = ImplicitOrderingSuite.basicMapExpectations(rdd)
    basicMapExpectations.map({case (met, explain) => assert(met, explain)})

    // Infer orderings for other RDD methods
    //对其他RDD方法进行排序
    val otherRDDMethodExpectations = ImplicitOrderingSuite.otherRDDMethodExpectations(rdd)
    otherRDDMethodExpectations.map({case (met, explain) => assert(met, explain)})
  }
}

private object ImplicitOrderingSuite {
  class NonOrderedClass {}

  class ComparableClass extends Comparable[ComparableClass] {
    override def compareTo(o: ComparableClass): Int = throw new UnsupportedOperationException
  }

  class OrderedClass extends Ordered[OrderedClass] {
    override def compare(o: OrderedClass): Int = throw new UnsupportedOperationException
  }

  def basicMapExpectations(rdd: RDD[Int]): List[(Boolean, String)] = {
    List((rdd.map(x => (x, x)).keyOrdering.isDefined,
            "rdd.map(x => (x, x)).keyOrdering.isDefined"),
          (rdd.map(x => (1, x)).keyOrdering.isDefined,
            "rdd.map(x => (1, x)).keyOrdering.isDefined"),
          (rdd.map(x => (x.toString, x)).keyOrdering.isDefined,
            "rdd.map(x => (x.toString, x)).keyOrdering.isDefined"),
          (rdd.map(x => (null, x)).keyOrdering.isDefined,
            "rdd.map(x => (null, x)).keyOrdering.isDefined"),
          (rdd.map(x => (new NonOrderedClass, x)).keyOrdering.isEmpty,
            "rdd.map(x => (new NonOrderedClass, x)).keyOrdering.isEmpty"),
          (rdd.map(x => (new ComparableClass, x)).keyOrdering.isDefined,
            "rdd.map(x => (new ComparableClass, x)).keyOrdering.isDefined"),
          (rdd.map(x => (new OrderedClass, x)).keyOrdering.isDefined,
            "rdd.map(x => (new OrderedClass, x)).keyOrdering.isDefined"))
  }

  def otherRDDMethodExpectations(rdd: RDD[Int]): List[(Boolean, String)] = {
    List((rdd.groupBy(x => x).keyOrdering.isDefined,
           "rdd.groupBy(x => x).keyOrdering.isDefined"),
         (rdd.groupBy(x => new NonOrderedClass).keyOrdering.isEmpty,
           "rdd.groupBy(x => new NonOrderedClass).keyOrdering.isEmpty"),
         (rdd.groupBy(x => new ComparableClass).keyOrdering.isDefined,
           "rdd.groupBy(x => new ComparableClass).keyOrdering.isDefined"),
         (rdd.groupBy(x => new OrderedClass).keyOrdering.isDefined,
           "rdd.groupBy(x => new OrderedClass).keyOrdering.isDefined"),
         (rdd.groupBy((x: Int) => x, 5).keyOrdering.isDefined,
           "rdd.groupBy((x: Int) => x, 5).keyOrdering.isDefined"),
         (rdd.groupBy((x: Int) => x, new HashPartitioner(5)).keyOrdering.isDefined,
           "rdd.groupBy((x: Int) => x, new HashPartitioner(5)).keyOrdering.isDefined"))
  }
}
