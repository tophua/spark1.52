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

package org.apache.spark.rdd

import java.io.{ObjectInputStream, ObjectOutputStream, IOException}

import com.esotericsoftware.kryo.KryoException

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.rdd.RDDSuiteUtils._
import org.apache.spark.util.Utils

class RDDSuite extends SparkFunSuite with SharedSparkContext {


  test("basic operations") {//基本操作
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)

    assert(nums.collect().toList === List(1, 2, 3, 4))
    
    assert(nums.toLocalIterator.toList === List(1, 2, 3, 4))
    val dups = sc.makeRDD(Array(1, 1, 2, 2, 3, 3, 4, 4), 2)
    assert(dups.distinct().count() === 4) //去除冗余元素

    assert(dups.distinct().count === 4)  // Can distinct and count be called without parentheses?
    assert(dups.distinct().collect === dups.distinct().collect)
    //分区数
    assert(dups.distinct(2).collect === dups.distinct().collect)
    //reduce接收一个函数,作用在两个类型 相同的元素上,返回一个类型相同的元素
    //最常用的一个函数是加法
   //合并RDD中的元素
    assert(nums.reduce(_ + _) === 10)
    //fold操作用于对RDD中的元素进行迭代操作,并且利用了一个变量保存迭代过程中的中间结果
    //与reduce 相似,在每个分区的初始化调用的时候多了个"0"值
    assert(nums.fold(0)(_ + _) === 10)
    //map 是把 function 作用到每个 element,针对的是 element
    assert(nums.map(_.toString).collect().toList === List("1", "2", "3", "4"))
    assert(nums.filter(_ > 2).collect().toList === List(3, 4))
    assert(nums.flatMap(x => 1 to x).collect().toList === List(1, 1, 2, 1, 2, 3, 1, 2, 3, 4))
    //两个RDD合为一个
    assert(nums.union(nums).collect().toList === List(1, 2, 3, 4, 1, 2, 3, 4))
    //glom函数将每个分区形成一个数组,内部实现是返回的GlommedRDD
    assert(nums.glom().map(_.toList).collect().toList === List(List(1, 2), List(3, 4)))
    //
    assert(nums.collect(
        { case i if i >= 3 
           => i.toString }).collect().toList === List("3", "4"))
    //创建一个健值对数组
    assert(nums.keyBy(_.toString).collect().toList === List(("1", 1), ("2", 2), ("3", 3), ("4", 4)))
    assert(!nums.isEmpty())
    assert(nums.max() === 4)
    assert(nums.min() === 1)
    //mapPartitions 是把 function 作用到每个 partition,针对的是 partition 内部的 iterator
    //mapPartitions 如果在映射的过程中需要频繁创建额外的对象,map就显得不高效了
    //map的输入函数是应用于RDD中每个元素,而mapPartitions的输入函数是应用于每个分区,
    // 也就是把每个分区中的内容作为整体来处理
    val partitionSums = nums.mapPartitions(iter => Iterator(iter.reduceLeft(_ + _)))
    //第一个分区是和3,第二个分区是和 7
    assert(partitionSums.collect().toList === List(3, 7))
    //mapPartitions函数会对每个分区依次调用分区函数处理,然后将处理的结果(若干个Iterator)生成新的RDDs
    val partitionSumsWithSplit = nums.mapPartitionsWithIndex {
      case(split, iter) => Iterator((split, iter.reduceLeft(_ + _)))
    }
    assert(partitionSumsWithSplit.collect().toList === List((0, 3), (1, 7)))

    val partitionSumsWithIndex = nums.mapPartitionsWithIndex {
      case(split, iter) => Iterator((split, iter.reduceLeft(_ + _)))
    }
    assert(partitionSumsWithIndex.collect().toList === List((0, 3), (1, 7)))

    intercept[UnsupportedOperationException] {
      nums.filter(_ > 5).reduce(_ + _)
    }
  }

  test("serialization") {//序列化
    val empty = new EmptyRDD[Int](sc)
    val serial = Utils.serialize(empty)
    val deserial: EmptyRDD[Int] = Utils.deserialize(serial)
    assert(!deserial.toString().isEmpty())
  }

  test("countApproxDistinct") {//计数相似

    def error(est: Long, size: Long): Double = math.abs(est - size) / size.toDouble

    val size = 1000
    val uniformDistro = for (i <- 1 to 5000) yield i % size
    val simpleRdd = sc.makeRDD(uniformDistro, 10)
    //countApproxDistinct : RDD的一个方法,作用是对RDD集合内容进行去重统计。
    //该统计是一个大约的统计,参数relativeSD控制统计的精确度。 relativeSD越小,结果越准确
    
    val a=simpleRdd.countApproxDistinct(8, 0)
    val b=simpleRdd.countApproxDistinct(12, 0)
    val c=simpleRdd.countApproxDistinct(0.02)
    val d=simpleRdd.countApproxDistinct(0.5)
    assert(error(simpleRdd.countApproxDistinct(8, 0), size) < 0.2)
    assert(error(simpleRdd.countApproxDistinct(12, 0), size) < 0.1)
    assert(error(simpleRdd.countApproxDistinct(0.02), size) < 0.1)
    assert(error(simpleRdd.countApproxDistinct(0.5), size) < 0.22)
  }

  test("SparkContext.union") {//联合
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
    //执行集合合并操作,不去重
    assert(sc.union(nums).collect().toList === List(1, 2, 3, 4))
    assert(sc.union(nums, nums).collect().toList === List(1, 2, 3, 4, 1, 2, 3, 4))
    assert(sc.union(Seq(nums)).collect().toList === List(1, 2, 3, 4))
    assert(sc.union(Seq(nums, nums)).collect().toList === List(1, 2, 3, 4, 1, 2, 3, 4))
  }

  test("SparkContext.union creates UnionRDD if at least one RDD has no partitioner") {
    val rddWithPartitioner = sc.parallelize(Seq(1->true)).partitionBy(new HashPartitioner(1))
    val rddWithNoPartitioner = sc.parallelize(Seq(2->true))
    val unionRdd = sc.union(rddWithNoPartitioner, rddWithPartitioner)
    assert(unionRdd.isInstanceOf[UnionRDD[_]])
  }

  test("SparkContext.union creates PartitionAwareUnionRDD if all RDDs have partitioners") {
    val rddWithPartitioner = sc.parallelize(Seq(1->true)).partitionBy(new HashPartitioner(1))
    val unionRdd = sc.union(rddWithPartitioner, rddWithPartitioner)
    assert(unionRdd.isInstanceOf[PartitionerAwareUnionRDD[_]])
  }

  test("PartitionAwareUnionRDD raises exception if at least one RDD has no partitioner") {
    val rddWithPartitioner = sc.parallelize(Seq(1->true)).partitionBy(new HashPartitioner(1))
    val rddWithNoPartitioner = sc.parallelize(Seq(2->true))
    intercept[IllegalArgumentException] {
      new PartitionerAwareUnionRDD(sc, Seq(rddWithNoPartitioner, rddWithPartitioner))
    }
  }

  test("partitioner aware union") {//分区清楚联合
    def makeRDDWithPartitioner(seq: Seq[Int]): RDD[Int] = {
      sc.makeRDD(seq, 1)
        .map(x => (x, null))
        .partitionBy(new HashPartitioner(2))
        .mapPartitions(_.map(_._1), true)
    }

    val nums1 = makeRDDWithPartitioner(1 to 4)
    val nums2 = makeRDDWithPartitioner(5 to 8)
    assert(nums1.partitioner == nums2.partitioner)
    assert(new PartitionerAwareUnionRDD(sc, Seq(nums1)).collect().toSet === Set(1, 2, 3, 4))

    val union = new PartitionerAwareUnionRDD(sc, Seq(nums1, nums2))
    assert(union.collect().toSet === Set(1, 2, 3, 4, 5, 6, 7, 8))
    val nums1Parts = nums1.collectPartitions()
    val nums2Parts = nums2.collectPartitions()
    val unionParts = union.collectPartitions()
    assert(nums1Parts.length === 2)
    assert(nums2Parts.length === 2)
    assert(unionParts.length === 2)
    assert((nums1Parts(0) ++ nums2Parts(0)).toList === unionParts(0).toList)
    assert((nums1Parts(1) ++ nums2Parts(1)).toList === unionParts(1).toList)
    assert(union.partitioner === nums1.partitioner)
  }  
  //合并分区尺寸要小
  test("UnionRDD partition serialized size should be small") {
    val largeVariable = new Array[Byte](1000 * 1000)
    val rdd1 = sc.parallelize(1 to 10, 2).map(i => largeVariable.length)
    val rdd2 = sc.parallelize(1 to 10, 3)

    val ser = SparkEnv.get.closureSerializer.newInstance()
    val union = rdd1.union(rdd2)
    // The UnionRDD itself should be large, but each individual partition should be small.
    assert(ser.serialize(union).limit() > 2000)
    assert(ser.serialize(union.partitions.head).limit() < 2000)
  }

  test("aggregate") {//聚合
    //与fold相似需要提供两个函数,返回不类似
    //第一个数据：RDD中元素累加,每个节点只累加本地的结果
    //第二个数据：合并累加器
    val pairs = sc.makeRDD(Array(("a", 1), ("b", 2), ("a", 2), ("c", 5), ("a", 3)))
    type StringMap = HashMap[String, Int]
    val emptyMap = new StringMap {
      override def default(key: String): Int = 0
    }
    //合并元素,匿名函数箭头左边是参数列表,右边是函数体
    val mergeElement: (StringMap, (String, Int)) => StringMap = (map, pair) => {
      map(pair._1) += pair._2
      map
    }
    //合并值,匿名函数箭头左边是参数列表,右边是函数体
    val mergeMaps: (StringMap, StringMap) => StringMap = (map1, map2) => {
      for ((key, value) <- map2) {
        map1(key) += value
      }
      map1
    }
    val result = pairs.aggregate(emptyMap)(mergeElement, mergeMaps)
    assert(result.toSet === Set(("a", 6), ("b", 2), ("c", 5)))
  }

  test("treeAggregate") {//树的聚合
    val rdd = sc.makeRDD(-1000 until 1000, 10)
    def seqOp: (Long, Int) => Long = (c: Long, x: Int) => c + x
    def combOp: (Long, Long) => Long = (c1: Long, c2: Long) => c1 + c2
    for (depth <- 1 until 10) {
      val sum = rdd.treeAggregate(0L)(seqOp, combOp, depth)
      assert(sum === -1000L)
    }
  }

  test("treeReduce") {//树的计算
    val rdd = sc.makeRDD(-1000 until 1000, 10)
    for (depth <- 1 until 10) {
      val sum = rdd.treeReduce(_ + _, depth)
      assert(sum === -1000)
    }
  }

  test("basic caching") {//基本缓存
    val rdd = sc.makeRDD(Array(1, 2, 3, 4), 2).cache()
    assert(rdd.collect().toList === List(1, 2, 3, 4))
    assert(rdd.collect().toList === List(1, 2, 3, 4))
    assert(rdd.collect().toList === List(1, 2, 3, 4))
  }

  test("caching with failures") {//缓存失败
    val onlySplit = new Partition { override def index: Int = 0 }
    var shouldFail = true
    val rdd = new RDD[Int](sc, Nil) {
      override def getPartitions: Array[Partition] = Array(onlySplit)
      override val getDependencies = List[Dependency[_]]()
      override def compute(split: Partition, context: TaskContext): Iterator[Int] = {
        //RDD计算失败,抛出异常,则缓存失败
        throw new Exception("injected failure")//注射失败
      }
    }.cache()
    val thrown = intercept[Exception]{
      rdd.collect()
    }
    assert(thrown.getMessage.contains("injected failure"))
  }

  test("empty RDD") {//空的RDD
    val empty = new EmptyRDD[Int](sc)
    assert(empty.count === 0)
    assert(empty.collect().size === 0)

    val thrown = intercept[UnsupportedOperationException]{
      empty.reduce(_ + _)
    }
    assert(thrown.getMessage.contains("empty"))

    val emptyKv = new EmptyRDD[(Int, Int)](sc)
    val rdd = sc.parallelize(1 to 2, 2).map(x => (x, x))
    //join 笛卡尔积
    assert(rdd.join(emptyKv).collect().size === 0)//与rrd join数据还是0 
    assert(rdd.rightOuterJoin(emptyKv).collect().size === 0)
    assert(rdd.leftOuterJoin(emptyKv).collect().size === 2)
    assert(rdd.fullOuterJoin(emptyKv).collect().size === 2)
    assert(rdd.cogroup(emptyKv).collect().size === 2)
    assert(rdd.union(emptyKv).collect().size === 2)//
  }
  //重新分区RDD
  test("repartitioned RDDs") {
    
    val data = sc.parallelize(1 to 1000, 10)
    //repartition 该函数用于将RDD进行重分区 该函数其实就是coalesce函数第二个参数为true的实现
    // Coalesce partitions 合并分区
    val repartitioned1 = data.repartition(2)
     
    assert(repartitioned1.partitions.size == 2)
    //glom函数将每个分区形成一个数组
    val partitions1 = repartitioned1.glom().collect()
    assert(partitions1(0).length > 0)
    assert(partitions1(1).length > 0)
    assert(repartitioned1.collect().toSet === (1 to 1000).toSet)

    // Split partitions
    //分隔分区
    val repartitioned2 = data.repartition(20)//增加分区,提高并行度
    assert(repartitioned2.partitions.size == 20)
    val partitions2 = repartitioned2.glom().collect()
    assert(partitions2(0).length > 0)
    assert(partitions2(19).length > 0)
    assert(repartitioned2.collect().toSet === (1 to 1000).toSet)
  }

  //重新分区执行负载均衡
  test("repartitioned RDDs perform load balancing") {
    // Coalesce partitions
    //合并分区
    val input = Array.fill(1000)(1)
    val initialPartitions = 10
    val data = sc.parallelize(input, initialPartitions)

    val repartitioned1 = data.repartition(2)
    assert(repartitioned1.partitions.size == 2)
    //glom把每个分区的数据合并成一个Array
    val partitions1 = repartitioned1.glom().collect()
    // some noise in balancing is allowed due to randomization
    //在平衡中的一些噪声被允许由于随机
    assert(math.abs(partitions1(0).length - 500) < initialPartitions)
    assert(math.abs(partitions1(1).length - 500) < initialPartitions)
    assert(repartitioned1.collect() === input)

    def testSplitPartitions(input: Seq[Int], initialPartitions: Int, finalPartitions: Int) {
      val data = sc.parallelize(input, initialPartitions)
      val repartitioned = data.repartition(finalPartitions)
      assert(repartitioned.partitions.size === finalPartitions)
      val partitions = repartitioned.glom().collect()
      // assert all elements are present
      //断言所有元素都存在
      assert(repartitioned.collect().sortWith(_ > _).toSeq === input.toSeq.sortWith(_ > _).toSeq)
      // assert no bucket is overloaded
      //断言没有一桶超载
      for (partition <- partitions) {
        val avg = input.size / finalPartitions
        val maxPossible = avg + initialPartitions
        assert(partition.length <=  maxPossible)
      }
    }

    testSplitPartitions(Array.fill(100)(1), 10, 20)
    testSplitPartitions(Array.fill(10000)(1) ++ Array.fill(10000)(2), 20, 100)
  }

  test("coalesced RDDs") {//合并RDD
    //联合,合并 coalesced
    val data = sc.parallelize(1 to 10, 10)
    //coalesce函数用于将RDD进行重分区,使用HashPartitioner。
        //第一个参数为重分区的数目,第二个为是否进行shuffle,默认为false;
    
    val coalesced1 = data.coalesce(2) //分区减少
    assert(coalesced1.collect().toList === (1 to 10).toList)
    assert(coalesced1.glom().collect().map(_.toList).toList ===
      List(List(1, 2, 3, 4, 5), List(6, 7, 8, 9, 10)))
     //dependencies RDD依赖关系 NarrowDependency(窄依赖关系)
    // Check that the narrow dependency is also specified correctly
      
    assert(coalesced1.dependencies.head.asInstanceOf[NarrowDependency[_]].getParents(0).toList ===
      List(0, 1, 2, 3, 4))
    assert(coalesced1.dependencies.head.asInstanceOf[NarrowDependency[_]].getParents(1).toList ===
      List(5, 6, 7, 8, 9))

    val coalesced2 = data.coalesce(3)
    assert(coalesced2.collect().toList === (1 to 10).toList)
    assert(coalesced2.glom().collect().map(_.toList).toList ===
      List(List(1, 2, 3), List(4, 5, 6), List(7, 8, 9, 10)))

    val coalesced3 = data.coalesce(10)
    assert(coalesced3.collect().toList === (1 to 10).toList)
    assert(coalesced3.glom().collect().map(_.toList).toList ===
      (1 to 10).map(x => List(x)).toList)

    // If we try to coalesce into more partitions than the original RDD, it should just
    // keep the original number of partitions.
    //如果我们试图合并更多的分区比原来的RDD,它应该只保留原来的分区数
    val coalesced4 = data.coalesce(20)//增加分区无效
    assert(coalesced4.partitions.size === 10)
    assert(coalesced4.collect().toList === (1 to 10).toList)
    assert(coalesced4.glom().collect().map(_.toList).toList ===
      (1 to 10).map(x => List(x)).toList)

    // we can optionally shuffle to keep the upstream parallel
    //我们可以选择Shuffle保持上游平行
    val coalesced5 = data.coalesce(1, shuffle = true)
    val isEquals = coalesced5.dependencies.head.rdd.dependencies.head.rdd.
      asInstanceOf[ShuffledRDD[_, _, _]] != null
    assert(isEquals)
  //如果重分区的数目大于原来的分区数,那么必须指定shuffle参数为true,//否则,分区数不变
    // when shuffling, we can increase the number of partitions
    val coalesced6 = data.coalesce(20, shuffle = true)
    assert(coalesced6.partitions.size === 20)
    assert(coalesced6.collect().toSet === (1 to 10).toSet)
  }
  //coalesced 合并本地的RDD
  test("coalesced RDDs with locality") {
    val data3 = sc.makeRDD(List((1, List("a", "c")), (2, List("a", "b", "c")), (3, List("b"))))
    val coal3 = data3.coalesce(3)
    //preferredLocation（最佳位置） 返回每一个数据块所在的机器名或者IP地址,如果每一块数据是多分存储的,那么就会返回多个机器地址
    val list3 = coal3.partitions.flatMap(_.asInstanceOf[CoalescedRDDPartition].preferredLocation)
    assert(list3.sorted === Array("a", "b", "c"), "Locality preferences are dropped")

    // RDD with locality preferences spread (non-randomly) over 6 machines, m0 through m5
    //RDD本地偏好传播(非随机)超过6的机器,M0通过M5
    val data = sc.makeRDD((1 to 9).map(i => (i, (i to (i + 2)).map{ j => "m" + (j%6)})))
    val coalesced1 = data.coalesce(3)
    assert(coalesced1.collect().toList.sorted === (1 to 9).toList, "Data got *lost* in coalescing")

    val splits = coalesced1.glom().collect().map(_.toList).toList
    assert(splits.length === 3, "Supposed to coalesce to 3 but got " + splits.length)

    assert(splits.forall(_.length >= 1) === true, "Some partitions were empty")

    // If we try to coalesce into more partitions than the original RDD, it should just
    // keep the original number of partitions.
    val coalesced4 = data.coalesce(20)
    val listOfLists = coalesced4.glom().collect().map(_.toList).toList
    val sortedList = listOfLists.sortWith{ (x, y) => !x.isEmpty && (y.isEmpty || (x(0) < y(0))) }
    assert(sortedList === (1 to 9).
      map{x => List(x)}.toList, "Tried coalescing 9 partitions to 20 but didn't get 9 back")
  }
  //合并本地的RDD,大数据
  test("coalesced RDDs with locality, large scale (10K partitions)") {
    // large scale experiment
    //大规模试验
    import collection.mutable
    val partitions = 10000
    val numMachines = 50
    val machines = mutable.ListBuffer[String]()
    (1 to numMachines).foreach(machines += "m" + _)
    val rnd = scala.util.Random
    for (seed <- 1 to 5) {
      rnd.setSeed(seed)

      val blocks = (1 to partitions).map { i =>
        (i, Array.fill(3)(machines(rnd.nextInt(machines.size))).toList)
      }

      val data2 = sc.makeRDD(blocks)
      val coalesced2 = data2.coalesce(numMachines * 2)

      // test that you get over 90% locality in each group
      //测试您在每个组中获得超过90%个位置
      val minLocality = coalesced2.partitions
        .map(part => part.asInstanceOf[CoalescedRDDPartition].localFraction)
        .foldLeft(1.0)((perc, loc) => math.min(perc, loc))
      assert(minLocality >= 0.90, "Expected 90% locality but got " +
        (minLocality * 100.0).toInt + "%")

      // test that the groups are load balanced with 100 +/- 20 elements in each
      //测试组负载平衡100 + / - 20个元素在每个
      val maxImbalance = coalesced2.partitions
        .map(part => part.asInstanceOf[CoalescedRDDPartition].parents.size)
        .foldLeft(0)((dev, curr) => math.max(math.abs(100 - curr), dev))
      assert(maxImbalance <= 20, "Expected 100 +/- 20 per partition, but got " + maxImbalance)
      //衍生的RDD测试*电流*优先的位置
      val data3 = sc.makeRDD(blocks).map(i => i * 2) // derived RDD to test *current* pref locs
      val coalesced3 = data3.coalesce(numMachines * 2)
      val minLocality2 = coalesced3.partitions
        .map(part => part.asInstanceOf[CoalescedRDDPartition].localFraction)
        .foldLeft(1.0)((perc, loc) => math.min(perc, loc))
      assert(minLocality2 >= 0.90, "Expected 90% locality for derived RDD but got " +
        (minLocality2 * 100.0).toInt + "%")
    }
  }

  // Test for SPARK-2412 -- ensure that the second pass of the algorithm does not throw an exception
  //确保算法的第二遍不抛出一个异常
  test("coalesced RDDs with locality, fail first pass") {
    val initialPartitions = 1000
    val targetLen = 50
    val couponCount = 2 * (math.log(targetLen)*targetLen + targetLen + 0.5).toInt // = 492

    val blocks = (1 to initialPartitions).map { i =>
      (i, List(if (i > couponCount) "m2" else "m1"))
    }
    val data = sc.makeRDD(blocks)
    val coalesced = data.coalesce(targetLen)
    assert(coalesced.partitions.length == targetLen)
  }

  test("zipped RDDs") {//拉链RDDS
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
    //两个RDD分区数目一致,且每个分区数据条数一致,该函数将RDD中的元素和这个元素在RDD中的组合成键/值对。
    val zipped = nums.zip(nums.map(_ + 1.0))
    assert(zipped.glom().map(_.toList).collect().toList ===
      List(List((1, 2.0), (2, 3.0)), List((3, 4.0), (4, 5.0))))

    intercept[IllegalArgumentException] {
      nums.zip(sc.parallelize(1 to 4, 1)).collect()
    }

    intercept[SparkException] {
      nums.zip(sc.parallelize(1 to 5, 2)).collect()
    }
  }

  test("partition pruning") {//分区裁剪
    val data = sc.parallelize(1 to 10, 10)
    // Note that split number starts from 0, so > 8 means only 10th partition left.
    //请注意,分裂数从0开始,所以> 8只剩下第十个分区
    val prunedRdd = new PartitionPruningRDD(data, splitNum => splitNum > 8)
    assert(prunedRdd.partitions.size === 1)
    val prunedData = prunedRdd.collect()
    assert(prunedData.size === 1)
    assert(prunedData(0) === 10)
  }

  test("mapWith") {//
    import java.util.Random
    val ones = sc.makeRDD(Array(1, 1, 1, 1, 1, 1), 2)
    @deprecated("suppress compile time deprecation warning", "1.0.0")
    val randoms = ones.mapWith(
      (index: Int) => new Random(index + 42))
      {(t: Int, prng: Random) => prng.nextDouble * t}.collect()
    val prn42_3 = {
      val prng42 = new Random(42)
      prng42.nextDouble(); prng42.nextDouble(); prng42.nextDouble()
    }
    val prn43_3 = {
      val prng43 = new Random(43)
      prng43.nextDouble(); prng43.nextDouble(); prng43.nextDouble()
    }
    assert(randoms(2) === prn42_3)
    assert(randoms(5) === prn43_3)
  }

  test("flatMapWith") {
    import java.util.Random
    val ones = sc.makeRDD(Array(1, 1, 1, 1, 1, 1), 2)
    @deprecated("suppress compile time deprecation warning", "1.0.0")
    val randoms = ones.flatMapWith(
      (index: Int) => new Random(index + 42))
      {(t: Int, prng: Random) =>
        val random = prng.nextDouble()
        Seq(random * t, random * t * 10)}.
      collect()
    val prn42_3 = {
      val prng42 = new Random(42)
      prng42.nextDouble(); prng42.nextDouble(); prng42.nextDouble()
    }
    val prn43_3 = {
      val prng43 = new Random(43)
      prng43.nextDouble(); prng43.nextDouble(); prng43.nextDouble()
    }
    assert(randoms(5) === prn42_3 * 10)
    assert(randoms(11) === prn43_3 * 10)
  }

  test("filterWith") {
    import java.util.Random
    val ints = sc.makeRDD(Array(1, 2, 3, 4, 5, 6), 2)
    @deprecated("suppress compile time deprecation warning", "1.0.0")
    val sample = ints.filterWith(
      (index: Int) => new Random(index + 42))
      {(t: Int, prng: Random) => prng.nextInt(3) == 0}.
      collect()
    val checkSample = {
      val prng42 = new Random(42)
      val prng43 = new Random(43)
      Array(1, 2, 3, 4, 5, 6).filter{i =>
        if (i < 4) 0 == prng42.nextInt(3) else 0 == prng43.nextInt(3)
      }
    }
    assert(sample.size === checkSample.size)
    for (i <- 0 until sample.size) assert(sample(i) === checkSample(i))
  }

  test("collect large number of empty partitions") {//收集大量的空分区
    // Regression test for SPARK-4019
    assert(sc.makeRDD(0 until 10, 1000).repartition(2001).collect().toSet === (0 until 10).toSet)
  }

  test("take") {//取出
    var nums = sc.makeRDD(Range(1, 1000), 1)
    assert(nums.take(0).size === 0)//take取出数据从1开始
    assert(nums.take(1) === Array(1)) //返回前几个元素
    assert(nums.take(3) === Array(1, 2, 3))
    assert(nums.take(500) === (1 to 500).toArray)
    assert(nums.take(501) === (1 to 501).toArray)
    assert(nums.take(999) === (1 to 999).toArray)
    assert(nums.take(1000) === (1 to 999).toArray)

    nums = sc.makeRDD(Range(1, 1000), 2)
    assert(nums.take(0).size === 0)
    assert(nums.take(1) === Array(1))
    assert(nums.take(3) === Array(1, 2, 3))
    assert(nums.take(500) === (1 to 500).toArray)
    assert(nums.take(501) === (1 to 501).toArray)
    assert(nums.take(999) === (1 to 999).toArray)
    assert(nums.take(1000) === (1 to 999).toArray)

    nums = sc.makeRDD(Range(1, 1000), 100)
    assert(nums.take(0).size === 0)
    assert(nums.take(1) === Array(1))
    assert(nums.take(3) === Array(1, 2, 3))
    assert(nums.take(500) === (1 to 500).toArray)
    assert(nums.take(501) === (1 to 501).toArray)
    assert(nums.take(999) === (1 to 999).toArray)
    assert(nums.take(1000) === (1 to 999).toArray)

    nums = sc.makeRDD(Range(1, 1000), 1000)
    assert(nums.take(0).size === 0)
    assert(nums.take(1) === Array(1))
    assert(nums.take(3) === Array(1, 2, 3))
    assert(nums.take(500) === (1 to 500).toArray)
    assert(nums.take(501) === (1 to 501).toArray)
    assert(nums.take(999) === (1 to 999).toArray)
    assert(nums.take(1000) === (1 to 999).toArray)
  }

  test("top with predefined ordering") {//顶部预定义排序
    // predefined 预定义 排序
    val nums = Array.range(1, 100000)
    val ints = sc.makeRDD(scala.util.Random.shuffle(nums), 2)
    val topK = ints.top(5)
    assert(topK.size === 5)
    assert(topK === nums.reverse.take(5))//反向
  }

  test("top with custom ordering") {//顶部自定义排序
    val words = Vector("a", "b", "c", "d")
    //自定义排序
    implicit val ord = implicitly[Ordering[String]].reverse //字符串倒序方式
    val rdd = sc.makeRDD(words, 2)
    val topK = rdd.top(2)
    assert(topK.size === 2)
    assert(topK.sorted === Array("b", "a"))
  }

  test("takeOrdered with predefined ordering") {
    val nums = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val rdd = sc.makeRDD(nums, 2)
    val sortedLowerK = rdd.takeOrdered(5)//取前5个
    assert(sortedLowerK.size === 5)
    assert(sortedLowerK === Array(1, 2, 3, 4, 5))
  }

  test("takeOrdered with limit 0") {
    val nums = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val rdd = sc.makeRDD(nums, 2)
    val sortedLowerK = rdd.takeOrdered(0)
    assert(sortedLowerK.size === 0)
  }

  test("takeOrdered with custom ordering") {//取出自定义顺序
    val nums = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    implicit val ord = implicitly[Ordering[Int]].reverse //倒序
    val rdd = sc.makeRDD(nums, 2)
    val sortedTopK = rdd.takeOrdered(5)
    assert(sortedTopK.size === 5)
    assert(sortedTopK === Array(10, 9, 8, 7, 6))
    assert(sortedTopK === nums.sorted(ord).take(5))
  }

  test("isEmpty") {
    assert(sc.emptyRDD.isEmpty())
    assert(sc.parallelize(Seq[Int]()).isEmpty())
    assert(!sc.parallelize(Seq(1)).isEmpty())
    //判断是否为空值
    assert(sc.parallelize(Seq(1, 2, 3), 3).filter(_ < 0).isEmpty())
    assert(!sc.parallelize(Seq(1, 2, 3), 3).filter(_ > 1).isEmpty())
  }

  test("sample preserves partitioner") {//preserves保持
    val partitioner = new HashPartitioner(2)
    //partitionBy 为rdd设置新的分区结构
    val rdd = sc.parallelize(Seq((0, 1), (2, 3))).partitionBy(partitioner)
    for (withReplacement <- Seq(true, false)) {
      val sampled = rdd.sample(withReplacement, 1.0) //采样
      assert(sampled.partitioner === rdd.partitioner)
    }
  }

  test("takeSample") {
    val n = 1000000
    val data = sc.parallelize(1 to n, 2)

    for (num <- List(5, 20, 100)) {
      val sample = data.takeSample(withReplacement = false, num = num)
      assert(sample.size === num)        // Got exactly num elements 有个元素
      assert(sample.toSet.size === num)  // Elements are distinct 不同的元素
      assert(sample.forall(x => 1 <= x && x <= n), s"elements not in [1, $n]")
    }
    for (seed <- 1 to 5) {
      val sample = data.takeSample(withReplacement = false, 20, seed)
      assert(sample.size === 20)        // Got exactly 20 elements 得到20个元素
      assert(sample.toSet.size === 20)  // Elements are distinct
      assert(sample.forall(x => 1 <= x && x <= n), s"elements not in [1, $n]")
    }
    for (seed <- 1 to 5) {
      val sample = data.takeSample(withReplacement = false, 100, seed)
      assert(sample.size === 100)        // Got only 100 elements
      assert(sample.toSet.size === 100)  // Elements are distinct
      assert(sample.forall(x => 1 <= x && x <= n), s"elements not in [1, $n]")
    }
    for (seed <- 1 to 5) {
      val sample = data.takeSample(withReplacement = true, 20, seed)
      assert(sample.size === 20)        // Got exactly 20 elements
      assert(sample.forall(x => 1 <= x && x <= n), s"elements not in [1, $n]")
    }
    {
      val sample = data.takeSample(withReplacement = true, num = 20)
      assert(sample.size === 20)        // Got exactly 100 elements
      assert(sample.toSet.size <= 20, "sampling with replacement returned all distinct elements")
      assert(sample.forall(x => 1 <= x && x <= n), s"elements not in [1, $n]")
    }
    {
      val sample = data.takeSample(withReplacement = true, num = n)
      assert(sample.size === n)        // Got exactly 100 elements
      // Chance of getting all distinct elements is astronomically low, so test we got < 100
      assert(sample.toSet.size < n, "sampling with replacement returned all distinct elements")
      assert(sample.forall(x => 1 <= x && x <= n), s"elements not in [1, $n]")
    }
    for (seed <- 1 to 5) {
      val sample = data.takeSample(withReplacement = true, n, seed)
      assert(sample.size === n)        // Got exactly 100 elements
      // Chance of getting all distinct elements is astronomically low, so test we got < 100
      assert(sample.toSet.size < n, "sampling with replacement returned all distinct elements")
    }
    for (seed <- 1 to 5) {
      val sample = data.takeSample(withReplacement = true, 2 * n, seed)
      assert(sample.size === 2 * n)        // Got exactly 200 elements
      // Chance of getting all distinct elements is still quite low, so test we got < 100
      assert(sample.toSet.size < n, "sampling with replacement returned all distinct elements")
    }
  }

  test("takeSample from an empty rdd") {
    val emptySet = sc.parallelize(Seq.empty[Int], 2)
    val sample = emptySet.takeSample(false, 20, 1)
    assert(sample.length === 0)
  }

  test("randomSplit") {//随机分裂
    val n = 600
    val data = sc.parallelize(1 to n, 2)
    for(seed <- 1 to 5) {
      val splits = data.randomSplit(Array(1.0, 2.0, 3.0), seed)
      assert(splits.size == 3, "wrong number of splits")//错误数量的分裂
      assert(splits.flatMap(_.collect()).sorted.toList == data.collect().toList,
        "incomplete or wrong split")
      val s = splits.map(_.count())
      assert(math.abs(s(0) - 100) < 50) // std =  9.13
      assert(math.abs(s(1) - 200) < 50) // std = 11.55
      assert(math.abs(s(2) - 300) < 50) // std = 12.25
    }
  }

  test("runJob on an invalid partition") {//在运行一个无效分区的Job
    intercept[IllegalArgumentException] {
      sc.runJob(sc.parallelize(1 to 10, 2), {iter: Iterator[Int] => iter.size}, Seq(0, 1, 2))
    }
  }

  test("sort an empty RDD") {//分类空的分区
    val data = sc.emptyRDD[Int]
    assert(data.sortBy(x => x).collect() === Array.empty)
  }

  test("sortByKey") {
    //sortBy是对标准的RDD进行排序,
    val data = sc.parallelize(Seq("5|50|A", "4|60|C", "6|40|B"))

    val col1 = Array("4|60|C", "5|50|A", "6|40|B")
    val col2 = Array("6|40|B", "5|50|A", "4|60|C")
    val col3 = Array("5|50|A", "6|40|B", "4|60|C")
     //0 升序,1  降序,2 中间排序
    //第一个参数是一个函数,该函数的也有一个带T泛型的参数,返回类型和RDD中元素的类型是一致的
    //第二个参数是ascending,从字面的意思大家应该可以猜到,这参数决定排序后RDD中的元素是升序还是降序
    //默认是true,也就是升序
    assert(data.sortBy(_.split("\\|")(0)).collect() === col1)//按分隔的第一个值升序
    assert(data.sortBy(_.split("\\|")(1)).collect() === col2)//按分隔的第二个值升序
    assert(data.sortBy(_.split("\\|")(2)).collect() === col3)//按分隔的第三个值的升序
  }

  test("sortByKey ascending parameter") {//sortByKey指定排序参数
    //sortByKey函数是对PairRDD进行排序,也就是有Key和Value的RDD
    val data = sc.parallelize(Seq("5|50|A", "4|60|C", "6|40|B"))

    val asc = Array("4|60|C", "5|50|A", "6|40|B")
    val desc = Array("6|40|B", "5|50|A", "4|60|C")
    //默认是true,也就是升序
    assert(data.sortBy(_.split("\\|")(0), true).collect() === asc)//按第一个值升序
    assert(data.sortBy(_.split("\\|")(0), false).collect() === desc)//按第一个值降序
  }

  test("sortByKey with explicit ordering") {//sortByKey显式排序
    val data = sc.parallelize(Seq("Bob|Smith|50",
                                  "Jane|Smith|40",
                                  "Thomas|Williams|30",
                                  "Karen|Williams|60"))

    val ageOrdered = Array("Thomas|Williams|30",
                           "Jane|Smith|40",
                           "Bob|Smith|50",
                           "Karen|Williams|60")

    // last name, then first name
    //最后的名称,第一名称
    val nameOrdered = Array("Bob|Smith|50",
                            "Jane|Smith|40",
                            "Karen|Williams|60",
                            "Thomas|Williams|30")
  //匿名函数箭头左边是参数列表,右边是函数体
    val parse = (s: String) => {
      val split = s.split("\\|")
      Person(split(0), split(1), split(2).toInt)
    }

    import scala.reflect.classTag
    //使用Person 年龄排序
    assert(data.sortBy(parse, true, 2)(AgeOrdering, classTag[Person]).collect() === ageOrdered)
    //使用名称排序
    assert(data.sortBy(parse, true, 2)(NameOrdering, classTag[Person]).collect() === nameOrdered)
  }

  test("repartitionAndSortWithinPartitions") {//在分区的划分和排序
    val data = sc.parallelize(Seq((0, 5), (3, 8), (2, 6), (0, 8), (3, 8), (1, 3)), 2)

    val partitioner = new Partitioner {
      def numPartitions: Int = 2
      def getPartition(key: Any): Int = key.asInstanceOf[Int] % 2
    }

    val repartitioned = data.repartitionAndSortWithinPartitions(partitioner)
    val partitions = repartitioned.glom().collect()
    assert(partitions(0) === Seq((0, 5), (0, 8), (2, 6)))
    assert(partitions(1) === Seq((1, 3), (3, 8), (3, 8)))
  }

  test("intersection") {//相交的集合
    //两个集合取相交的集合,并去重
    val all = sc.parallelize(1 to 10)
    val evens = sc.parallelize(2 to 10 by 2)
    //相交的集合
    val intersection = Array(2, 4, 6, 8, 10)

    // intersection is commutative
    //两个集合取相交的集合,并去重
    assert(all.intersection(evens).collect().sorted === intersection)
    assert(evens.intersection(all).collect().sorted === intersection)
  }

  test("intersection strips duplicates in an input") {//输入重复相交的集合
    val a = sc.parallelize(Seq(1, 2, 3, 3))
    val b = sc.parallelize(Seq(1, 1, 2, 3))
    //相交的集合
    val intersection = Array(1, 2, 3)
     //两个集合取相交的集合,并去重
    assert(a.intersection(b).collect().sorted === intersection)
    assert(b.intersection(a).collect().sorted === intersection)
  }

  test("zipWithIndex") {    
    val n = 10
    val data = sc.parallelize(0 until n, 3)
    //该函数将RDD中的元素和这个元素在RDD中的ID(索引号)组合成键/值对
    val ranked = data.zipWithIndex()
    /**
     * ranked = Array((0,0), (1,1), (2,2), (3,3), (4,4), (5,5), (6,6), (7,7), (8,8), (9,9))
     */
    ranked.collect().foreach { x =>
      assert(x._1 === x._2)
    }
  }

  test("zipWithIndex with a single partition") {//单一个分区的索引
    val n = 10
    val data = sc.parallelize(0 until n, 1)
    val ranked = data.zipWithIndex()
    ranked.collect().foreach { x =>
      assert(x._1 === x._2)
    }
  }

  test("zipWithIndex chained with other RDDs (SPARK-4433)") {
    val count = sc.parallelize(0 until 10, 2).zipWithIndex().repartition(4).count()
    assert(count === 10)
  }

  test("zipWithUniqueId") {
    //该函数将RDD中元素和一个唯一ID组合成键/值对
    val n = 10
    val data = sc.parallelize(0 until n, 3)
    val ranked = data.zipWithUniqueId()
    /**
     * ranked = Array((0,0), (1,3), (2,6), (3,1), (4,4), (5,7), (6,2),(7,5), (8,8), (9,11))
     */
    val ids = ranked.map(_._1).distinct().collect()
    assert(ids.length === n)
  }

  test("retag with implicit ClassTag") {//隐式ClassTag
    val jsc: JavaSparkContext = new JavaSparkContext(sc)
    val jrdd: JavaRDD[String] = jsc.parallelize(Seq("A", "B", "C").asJava)
    jrdd.rdd.retag.collect()
  }

  test("parent method") {//父类方法
    val rdd1 = sc.parallelize(1 to 10, 2)
    val rdd2 = rdd1.filter(_ % 2 == 0)
    val rdd3 = rdd2.map(_ + 1)
    val rdd4 = new UnionRDD(sc, List(rdd1, rdd2, rdd3))
    assert(rdd4.parent(0).isInstanceOf[ParallelCollectionRDD[_]])
    assert(rdd4.parent[Int](1) === rdd2)
    assert(rdd4.parent[Int](2) === rdd3)
  }

  test("getNarrowAncestors") {//获得窄依赖
    val rdd1 = sc.parallelize(1 to 100, 4)
    val rdd2 = rdd1.filter(_ % 2 == 0).map(_ + 1)
    val rdd3 = rdd2.map(_ - 1).filter(_ < 50).map(i => (i, i))
    val rdd4 = rdd3.reduceByKey(_ + _)
    val rdd5 = rdd4.mapValues(_ + 1).mapValues(_ + 2).mapValues(_ + 3)
    val ancestors1 = rdd1.getNarrowAncestors//获得窄依赖
    val ancestors2 = rdd2.getNarrowAncestors//获得窄依赖
    val ancestors3 = rdd3.getNarrowAncestors//获得窄依赖
    val ancestors4 = rdd4.getNarrowAncestors//获得窄依赖
    val ancestors5 = rdd5.getNarrowAncestors//获得窄依赖

    // Simple dependency tree with a single branch
    //简单依赖树的一个分支
    assert(ancestors1.size === 0)
    assert(ancestors2.size === 2)
    assert(ancestors2.count(_ === rdd1) === 1)
    assert(ancestors2.count(_.isInstanceOf[MapPartitionsRDD[_, _]]) === 1)
    assert(ancestors3.size === 5)
    assert(ancestors3.count(_.isInstanceOf[MapPartitionsRDD[_, _]]) === 4)

    // Any ancestors before the shuffle are not considered
    //没有考虑Shuffle之前的任何祖先
    assert(ancestors4.size === 0)
    assert(ancestors4.count(_.isInstanceOf[ShuffledRDD[_, _, _]]) === 0)
    assert(ancestors5.size === 3)
    assert(ancestors5.count(_.isInstanceOf[ShuffledRDD[_, _, _]]) === 1)
    assert(ancestors5.count(_ === rdd3) === 0)
    assert(ancestors5.count(_.isInstanceOf[MapPartitionsRDD[_, _]]) === 2)
  }

  test("getNarrowAncestors with multiple parents") {//多个父窄依赖
    val rdd1 = sc.parallelize(1 to 100, 5)
    val rdd2 = sc.parallelize(1 to 200, 10).map(_ + 1)
    val rdd3 = sc.parallelize(1 to 300, 15).filter(_ > 50)
    val rdd4 = rdd1.map(i => (i, i))
    val rdd5 = rdd2.map(i => (i, i))
    val rdd6 = sc.union(rdd1, rdd2)
    val rdd7 = sc.union(rdd1, rdd2, rdd3)
    val rdd8 = sc.union(rdd6, rdd7)
    val rdd9 = rdd4.join(rdd5)
    val ancestors6 = rdd6.getNarrowAncestors
    val ancestors7 = rdd7.getNarrowAncestors
    val ancestors8 = rdd8.getNarrowAncestors
    val ancestors9 = rdd9.getNarrowAncestors

    // Simple dependency tree with multiple branches
    //多分支简单依赖树
    assert(ancestors6.size === 3)
    assert(ancestors6.count(_.isInstanceOf[ParallelCollectionRDD[_]]) === 2)
    assert(ancestors6.count(_ === rdd2) === 1)
    assert(ancestors7.size === 5)
    assert(ancestors7.count(_.isInstanceOf[ParallelCollectionRDD[_]]) === 3)
    assert(ancestors7.count(_ === rdd2) === 1)
    assert(ancestors7.count(_ === rdd3) === 1)

    // Dependency tree with duplicate nodes (e.g. rdd1 should not be reported twice)
    //具有重复节点的依赖树
    assert(ancestors8.size === 7)
    assert(ancestors8.count(_ === rdd2) === 1)
    assert(ancestors8.count(_ === rdd3) === 1)
    assert(ancestors8.count(_.isInstanceOf[UnionRDD[_]]) === 2)
    assert(ancestors8.count(_.isInstanceOf[ParallelCollectionRDD[_]]) === 3)
    assert(ancestors8.count(_ == rdd1) === 1)
    assert(ancestors8.count(_ == rdd2) === 1)
    assert(ancestors8.count(_ == rdd3) === 1)

    // Any ancestors before the shuffle are not considered
    //没有考虑Shuffle之前的任何祖先
    assert(ancestors9.size === 2)
    assert(ancestors9.count(_.isInstanceOf[CoGroupedRDD[_]]) === 1)
  }

  /**
   * This tests for the pathological condition in which the RDD dependency graph is cyclical.
   *
   * Since RDD is part of the public API, applications may actually implement RDDs that allow
   * such graphs to be constructed. In such cases, getNarrowAncestor should not simply hang.
   */
  test("getNarrowAncestors with cycles") {//循环窄依赖
    val rdd1 = new CyclicalDependencyRDD[Int]
    val rdd2 = new CyclicalDependencyRDD[Int]
    val rdd3 = new CyclicalDependencyRDD[Int]
    val rdd4 = rdd3.map(_ + 1).filter(_ > 10).map(_ + 2).filter(_ % 5 > 1)
    val rdd5 = rdd4.map(_ + 2).filter(_ > 20)
    val rdd6 = sc.union(rdd1, rdd2, rdd3).map(_ + 4).union(rdd5).union(rdd4)

    // Simple cyclical dependency 简单的循环依赖
    rdd1.addDependency(new OneToOneDependency[Int](rdd2))
    rdd2.addDependency(new OneToOneDependency[Int](rdd1))
    val ancestors1 = rdd1.getNarrowAncestors
    val ancestors2 = rdd2.getNarrowAncestors
    assert(ancestors1.size === 1)
    assert(ancestors1.count(_ == rdd2) === 1)
    assert(ancestors1.count(_ == rdd1) === 0)
    assert(ancestors2.size === 1)
    assert(ancestors2.count(_ == rdd1) === 1)
    assert(ancestors2.count(_ == rdd2) === 0)

    // Cycle involving a longer chain
    //循环较长的链
    rdd3.addDependency(new OneToOneDependency[Int](rdd4))
    val ancestors3 = rdd3.getNarrowAncestors
    val ancestors4 = rdd4.getNarrowAncestors
    assert(ancestors3.size === 4)
    assert(ancestors3.count(_.isInstanceOf[MapPartitionsRDD[_, _]]) === 4)
    assert(ancestors3.count(_ == rdd3) === 0)
    assert(ancestors4.size === 4)
    assert(ancestors4.count(_.isInstanceOf[MapPartitionsRDD[_, _]]) === 3)
    assert(ancestors4.count(_.isInstanceOf[CyclicalDependencyRDD[_]]) === 1)
    assert(ancestors4.count(_ == rdd3) === 1)
    assert(ancestors4.count(_ == rdd4) === 0)

    // Cycles that do not involve the root
    //不涉及根的周期
    val ancestors5 = rdd5.getNarrowAncestors
    assert(ancestors5.size === 6)
    assert(ancestors5.count(_.isInstanceOf[MapPartitionsRDD[_, _]]) === 5)
    assert(ancestors5.count(_.isInstanceOf[CyclicalDependencyRDD[_]]) === 1)
    assert(ancestors4.count(_ == rdd3) === 1)

    // Complex cyclical dependency graph (combination of all of the above)
    //复杂周期依赖关系图
    val ancestors6 = rdd6.getNarrowAncestors
    assert(ancestors6.size === 12)
    assert(ancestors6.count(_.isInstanceOf[UnionRDD[_]]) === 2)
    assert(ancestors6.count(_.isInstanceOf[MapPartitionsRDD[_, _]]) === 7)
    assert(ancestors6.count(_.isInstanceOf[CyclicalDependencyRDD[_]]) === 3)
  }
  //任务序列化异常不应挂起调度程序
  test("task serialization exception should not hang scheduler") {
    class BadSerializable extends Serializable {
      @throws(classOf[IOException])
      private def writeObject(out: ObjectOutputStream): Unit =
        throw new KryoException("Bad serialization")

      @throws(classOf[IOException])
      private def readObject(in: ObjectInputStream): Unit = {}
    }
    // Note that in the original bug, SPARK-4349, that this verifies, the job would only hang if
    // there were more threads in the Spark Context than there were number of objects in this
    // sequence.
    intercept[Throwable] {
      sc.parallelize(Seq(new BadSerializable, new BadSerializable)).collect()
    }
    // Check that the context has not crashed
    //检查上下文并没有崩溃
    sc.parallelize(1 to 100).map(x => x*2).collect
  }

  /** 
   *  A contrived RDD that allows the manual addition of dependencies after creation. 
   * 
   *  */
  private class CyclicalDependencyRDD[T: ClassTag] extends RDD[T](sc, Nil) {
    private val mutableDependencies: ArrayBuffer[Dependency[_]] = ArrayBuffer.empty
    override def compute(p: Partition, c: TaskContext): Iterator[T] = Iterator.empty
    override def getPartitions: Array[Partition] = Array.empty
    override def getDependencies: Seq[Dependency[_]] = mutableDependencies
    def addDependency(dep: Dependency[_]) {
      mutableDependencies += dep
    }
  }

  test("nested RDDs are not supported (SPARK-5063)") {//不支持嵌套RDDS
    val rdd: RDD[Int] = sc.parallelize(1 to 100)
    val rdd2: RDD[Int] = sc.parallelize(1 to 100)
    val thrown = intercept[SparkException] {
      val nestedRDD: RDD[RDD[Int]] = rdd.mapPartitions { x => Seq(rdd2.map(x => x)).iterator }
      nestedRDD.count()
    }
    assert(thrown.getMessage.contains("SPARK-5063"))
  }

  test("actions cannot be performed inside of transformations (SPARK-5063)") {//无法执行内部转换
    val rdd: RDD[Int] = sc.parallelize(1 to 100)
    val rdd2: RDD[Int] = sc.parallelize(1 to 100)
    val thrown = intercept[SparkException] {
      rdd.map(x => x * rdd2.count).collect()
    }
    assert(thrown.getMessage.contains("SPARK-5063"))
  }
  //无法运行操作之后sparkcontext已停止
  test("cannot run actions after SparkContext has been stopped (SPARK-5063)") {
    val existingRDD = sc.parallelize(1 to 100)
    sc.stop()
    val thrown = intercept[IllegalStateException] {
      existingRDD.count()
    }
    assert(thrown.getMessage.contains("shutdown"))
  }
  //无法调用方法在停止sparkcontext
  test("cannot call methods on a stopped SparkContext (SPARK-5063)") {
    sc.stop()
    def assertFails(block: => Any): Unit = {
      val thrown = intercept[IllegalStateException] {
        block
      }
      assert(thrown.getMessage.contains("stopped"))
    }
    assertFails { sc.parallelize(1 to 100) }
    assertFails { sc.textFile("/nonexistent-path") }
  }

}
