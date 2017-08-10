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

import scala.collection.mutable.ArrayBuffer
import scala.math.abs

import org.scalatest.PrivateMethodTester

import org.apache.spark.rdd.RDD
import org.apache.spark.util.StatCounter

class PartitioningSuite extends SparkFunSuite with SharedSparkContext with PrivateMethodTester {

  test("HashPartitioner equality") {//分区相等操作 
    val p2 = new HashPartitioner(2)
    val p4 = new HashPartitioner(4)
    val anotherP4 = new HashPartitioner(4)
    assert(p2 === p2)
    assert(p4 === p4)
    assert(p2 != p4)
    assert(p4 != p2)
    assert(p4 === anotherP4)
    assert(anotherP4 === p4)
  }

  test("RangePartitioner equality") {//范围分区相等比较
    // Make an RDD where all the elements are the same so that the partition range bounds
    // are deterministically all the same.
    //创建一个RDD,其中所有元素都相同,以便分区范围界限确定性都相同。
    val rdd = sc.parallelize(Seq(1, 1, 1, 1)).map(x => (x, x))

    val p2 = new RangePartitioner(2, rdd)
    val p4 = new RangePartitioner(4, rdd)
    val anotherP4 = new RangePartitioner(4, rdd)
    val descendingP2 = new RangePartitioner(2, rdd, false)
    val descendingP4 = new RangePartitioner(4, rdd, false)

    assert(p2 === p2)
    assert(p4 === p4)
    assert(p2 === p4)
    assert(p4 === anotherP4)
    assert(anotherP4 === p4)
    assert(descendingP2 === descendingP2)
    assert(descendingP4 === descendingP4)
    assert(descendingP2 === descendingP4)
    assert(p2 != descendingP2)
    assert(p4 != descendingP4)
    assert(descendingP2 != p2)
    assert(descendingP4 != p4)
  }
  //得到分区
  test("RangePartitioner getPartition") {
    val rdd = sc.parallelize(1.to(2000)).map(x => (x, x))
    // We have different behaviour of getPartition for partitions with less than 1000 and more than
    // 1000 partitions.
    //对于小于1000和超过1000个分区的分区,我们对getPartition有不同的行为
    val partitionSizes = List(1, 2, 10, 100, 500, 1000, 1500)
    val partitioners = partitionSizes.map(p => (p, new RangePartitioner(p, rdd)))
    val decoratedRangeBounds = PrivateMethod[Array[Int]]('rangeBounds)
    partitioners.map { case (numPartitions, partitioner) =>
      val rangeBounds = partitioner.invokePrivate(decoratedRangeBounds())
      1.to(1000).map { element => {
        val partition = partitioner.getPartition(element)
        if (numPartitions > 1) {
          if (partition < rangeBounds.size) {
            assert(element <= rangeBounds(partition))
          }
          if (partition > 0) {
            assert(element > rangeBounds(partition - 1))
          }
        } else {
          assert(partition === 0)
        }
      }}
    }
  }
  //对于不可比较的键,但与排序
  test("RangePartitioner for keys that are not Comparable (but with Ordering)") {
    // Row does not extend Comparable, but has an implicit Ordering defined.
    //行不扩展可比性,但有一个隐含的排序定义
    implicit object RowOrdering extends Ordering[Item] {
      override def compare(x: Item, y: Item): Int = x.value - y.value
    }

    val rdd = sc.parallelize(1 to 4500).map(x => (Item(x), Item(x)))
    val partitioner = new RangePartitioner(1500, rdd)
    partitioner.getPartition(Item(100))
  }

  test("RangPartitioner.sketch") {
    val rdd = sc.makeRDD(0 until 20, 20).flatMap { i =>
      val random = new java.util.Random(i)
      Iterator.fill(i)(random.nextDouble())
    }.cache()
    val sampleSizePerPartition = 10
    val (count, sketched) = RangePartitioner.sketch(rdd, sampleSizePerPartition)
    assert(count === rdd.count())
    sketched.foreach { case (idx, n, sample) =>
      assert(n === idx)
      assert(sample.size === math.min(n, sampleSizePerPartition))
    }
  }
  //确定界限
  test("RangePartitioner.determineBounds") {
    assert(RangePartitioner.determineBounds(ArrayBuffer.empty[(Int, Float)], 10).isEmpty,
      "Bounds on an empty candidates set should be empty.")
    val candidates = ArrayBuffer(
      (0.7, 2.0f), (0.1, 1.0f), (0.4, 1.0f), (0.3, 1.0f), (0.2, 1.0f), (0.5, 1.0f), (1.0, 3.0f))
    assert(RangePartitioner.determineBounds(candidates, 3) === Array(0.4, 0.7))
  }
  //如果数据基本平衡,只有一个运行Job
  test("RangePartitioner should run only one job if data is roughly balanced") {
    val rdd = sc.makeRDD(0 until 20, 20).flatMap { i =>
      val random = new java.util.Random(i)
      Iterator.fill(5000 * i)((random.nextDouble() + i, i))
    }.cache()
    for (numPartitions <- Seq(10, 20, 40)) {
      val partitioner = new RangePartitioner(numPartitions, rdd)
      assert(partitioner.numPartitions === numPartitions)
      val counts = rdd.keys.map(key => partitioner.getPartition(key)).countByValue().values
      assert(counts.max < 3.0 * counts.min)
    }
  }
  //应该在不平衡的数据上工作得很好
  test("RangePartitioner should work well on unbalanced data") {
    val rdd = sc.makeRDD(0 until 20, 20).flatMap { i =>
      val random = new java.util.Random(i)
      Iterator.fill(20 * i * i * i)((random.nextDouble() + i, i))
    }.cache()
    for (numPartitions <- Seq(2, 4, 8)) {
      val partitioner = new RangePartitioner(numPartitions, rdd)
      assert(partitioner.numPartitions === numPartitions)
      val counts = rdd.keys.map(key => partitioner.getPartition(key)).countByValue().values
      assert(counts.max < 3.0 * counts.min)
    }
  }
  //应返回空RDDS单个分区
  test("RangePartitioner should return a single partition for empty RDDs") {
    val empty1 = sc.emptyRDD[(Int, Double)]
    val partitioner1 = new RangePartitioner(0, empty1)
    assert(partitioner1.numPartitions === 1)
    val empty2 = sc.makeRDD(0 until 2, 2).flatMap(i => Seq.empty[(Int, Double)])
    val partitioner2 = new RangePartitioner(2, empty2)
    assert(partitioner2.numPartitions === 1)
  }

  test("HashPartitioner not equal to RangePartitioner") {
    val rdd = sc.parallelize(1 to 10).map(x => (x, x))
    val rangeP2 = new RangePartitioner(2, rdd)
    val hashP2 = new HashPartitioner(2)
    assert(rangeP2 === rangeP2)
    assert(hashP2 === hashP2)
    assert(hashP2 != rangeP2)
    assert(rangeP2 != hashP2)
  }

  test("partitioner preservation") {//保存分区
    val rdd = sc.parallelize(1 to 10, 4).map(x => (x, x))
      //groupByKey(2)重新设置分区
    val grouped2 = rdd.groupByKey(2)
    val grouped4 = rdd.groupByKey(4)
    val reduced2 = rdd.reduceByKey(_ + _, 2)
    val reduced4 = rdd.reduceByKey(_ + _, 4)

    assert(rdd.partitioner === None)

    assert(grouped2.partitioner === Some(new HashPartitioner(2)))
    assert(grouped4.partitioner === Some(new HashPartitioner(4)))
    assert(reduced2.partitioner === Some(new HashPartitioner(2)))
    assert(reduced4.partitioner === Some(new HashPartitioner(4)))

    assert(grouped2.groupByKey().partitioner  === grouped2.partitioner)
    assert(grouped2.groupByKey(3).partitioner !=  grouped2.partitioner)
    assert(grouped2.groupByKey(2).partitioner === grouped2.partitioner)
    assert(grouped4.groupByKey().partitioner  === grouped4.partitioner)
    assert(grouped4.groupByKey(3).partitioner !=  grouped4.partitioner)
    assert(grouped4.groupByKey(4).partitioner === grouped4.partitioner)
    //partitioner(4)
    assert(grouped2.join(grouped4).partitioner === grouped4.partitioner)
    //partitioner(4)
    assert(grouped2.leftOuterJoin(grouped4).partitioner === grouped4.partitioner)
    assert(grouped2.rightOuterJoin(grouped4).partitioner === grouped4.partitioner)
    assert(grouped2.fullOuterJoin(grouped4).partitioner === grouped4.partitioner)
    //partitioner(4)
    assert(grouped2.cogroup(grouped4).partitioner === grouped4.partitioner)

    //partitioner(2)
    assert(grouped2.join(reduced2).partitioner === grouped2.partitioner)
    assert(grouped2.leftOuterJoin(reduced2).partitioner === grouped2.partitioner)
    assert(grouped2.rightOuterJoin(reduced2).partitioner === grouped2.partitioner)
    assert(grouped2.fullOuterJoin(reduced2).partitioner === grouped2.partitioner)
    assert(grouped2.cogroup(reduced2).partitioner === grouped2.partitioner)

    //map 不产生分区
    assert(grouped2.map(_ => 1).partitioner === None)
    //mapValues 产生分区
    assert(grouped2.mapValues(_ => 1).partitioner === grouped2.partitioner)
    assert(grouped2.flatMapValues(_ => Seq(1)).partitioner === grouped2.partitioner)
    assert(grouped2.filter(_._1 > 4).partitioner === grouped2.partitioner)

  }
  //分区Java数组应该失败
  test("partitioning Java arrays should fail") {
    val arrs: RDD[Array[Int]] = sc.parallelize(Array(1, 2, 3, 4), 2).map(x => Array(x))
    val arrPairs: RDD[(Array[Int], Int)] =
      sc.parallelize(Array(1, 2, 3, 4), 2).map(x => (Array(x), x))

    def verify(testFun: => Unit): Unit = {
      intercept[SparkException](testFun).getMessage.contains("array")
    }

    verify(arrs.distinct())
    ///我们无法捕获数组的所有用法,因为它们可能发生在其他集合中：
    // We can't catch all usages of arrays, since they might occur inside other collections:
    // assert(fails { arrPairs.distinct() })
    verify(arrPairs.partitionBy(new HashPartitioner(2)))
    verify(arrPairs.join(arrPairs))
    verify(arrPairs.leftOuterJoin(arrPairs))
    verify(arrPairs.rightOuterJoin(arrPairs))
    verify(arrPairs.fullOuterJoin(arrPairs))
    verify(arrPairs.groupByKey())
    verify(arrPairs.countByKey())
    verify(arrPairs.countByKeyApprox(1))
    verify(arrPairs.cogroup(arrPairs))
    verify(arrPairs.reduceByKeyLocally(_ + _))
    verify(arrPairs.reduceByKey(_ + _))
  }

  test("zero-length partitions should be correctly handled") {//零长度分区应正确处理
    // Create RDD with some consecutive empty partitions (including the "first" one)
    //创建RDD与一些连续的空分区（包括“第一”）
    //一些连续的空分区创建RDD
    val rdd: RDD[Double] = sc
        .parallelize(Array(-1.0, -1.0, -1.0, -1.0, 2.0, 4.0, -1.0, -1.0), 8)
        .filter(_ >= 0.0)

    // Run the partitions, including the consecutive empty ones, through StatCounter
    //取出RDD数据值最大值,最小值,总数,中间值
    val stats: StatCounter = rdd.stats()
    assert(abs(6.0 - stats.sum) < 0.01)//
    assert(abs(6.0/2 - rdd.mean) < 0.01)
    assert(abs(1.0 - rdd.variance) < 0.01)
    assert(abs(1.0 - rdd.stdev) < 0.01)
    assert(stats.max === 4.0)
    assert(stats.min === 2.0)

    // Add other tests here for classes that should be able to handle empty partitions correctly
    //在这里添加其他类型的测试,以正确处理空分区
  }
}


private sealed case class Item(value: Int)
