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

package org.apache.spark.util.collection

import scala.collection.mutable.ArrayBuffer

import org.apache.spark._
import org.apache.spark.io.CompressionCodec

class ExternalAppendOnlyMapSuite extends SparkFunSuite with LocalSparkContext {
  private val allCompressionCodecs = CompressionCodec.ALL_COMPRESSION_CODECS
  private def createCombiner[T](i: T) = ArrayBuffer[T](i)
  private def mergeValue[T](buffer: ArrayBuffer[T], i: T): ArrayBuffer[T] = buffer += i
  private def mergeCombiners[T](buf1: ArrayBuffer[T], buf2: ArrayBuffer[T]): ArrayBuffer[T] =
    buf1 ++= buf2

  private def createExternalMap[T] = new ExternalAppendOnlyMap[T, T, ArrayBuffer[T]](
    createCombiner[T], mergeValue[T], mergeCombiners[T])

  private def createSparkConf(loadDefaults: Boolean, codec: Option[String] = None): SparkConf = {
    val conf = new SparkConf(loadDefaults)
    // Make the Java serializer write a reset instruction (TC_RESET) after each object to test
    // for a bug we had with bytes written past the last object in a batch (SPARK-2792)
    //使Java序列化程序在每个对象之后写入一个复位指令(TC_RESET),以测试一个错误,我们用一个批次中写入了最后一个对象的字节(SPARK-2792)
    conf.set("spark.serializer.objectStreamReset", "1")
    conf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    //spark.shuffle.spill用于指定Shuffle过程中如果内存中的数据超过阈值(参考spark.shuffle.memoryFraction的设置),
    //那么是否需要将部分数据临时写入外部存储。如果设置为false，那么这个过程就会一直使用内存
    conf.set("spark.shuffle.spill.compress", codec.isDefined.toString)
    conf.set("spark.shuffle.compress", codec.isDefined.toString)
    ////用于压缩内部数据如 RDD分区和shuffle输出的编码解码器
    codec.foreach { c => conf.set("spark.io.compression.codec", c) }
    // Ensure that we actually have multiple batches per spill file
    //确保每个溢出文件实际上有多个批次
    conf.set("spark.shuffle.spill.batchSize", "10")
    conf
  }

  test("simple insert") {//简单的插入
    val conf = createSparkConf(loadDefaults = false)
    sc = new SparkContext("local", "test", conf)
    val map = createExternalMap[Int]

    // Single insert
    //单个插入
    map.insert(1, 10)
    var it = map.iterator
    assert(it.hasNext)
    val kv = it.next()
    assert(kv._1 === 1 && kv._2 === ArrayBuffer[Int](10))
    assert(!it.hasNext)

    // Multiple insert
    //多个插入
    map.insert(2, 20)
    map.insert(3, 30)
    it = map.iterator
    assert(it.hasNext)
    assert(it.toSet === Set[(Int, ArrayBuffer[Int])](
      (1, ArrayBuffer[Int](10)),
      (2, ArrayBuffer[Int](20)),
      (3, ArrayBuffer[Int](30))))
    sc.stop()
  }

  test("insert with collision") {//插入冲突
    val conf = createSparkConf(loadDefaults = false)
    sc = new SparkContext("local", "test", conf)
    val map = createExternalMap[Int]

    map.insertAll(Seq(
      (1, 10),
      (2, 20),
      (3, 30),
      (1, 100),
      (2, 200),
      (1, 1000)))
    val it = map.iterator
    assert(it.hasNext)
    val result = it.toSet[(Int, ArrayBuffer[Int])].map(kv => (kv._1, kv._2.toSet))
    assert(result === Set[(Int, Set[Int])](
      (1, Set[Int](10, 100, 1000)),
      (2, Set[Int](20, 200)),
      (3, Set[Int](30))))
    sc.stop()
  }

  test("ordering") {//排序
    val conf = createSparkConf(loadDefaults = false)
    sc = new SparkContext("local", "test", conf)

    val map1 = createExternalMap[Int]
    map1.insert(1, 10)
    map1.insert(2, 20)
    map1.insert(3, 30)

    val map2 = createExternalMap[Int]
    map2.insert(2, 20)
    map2.insert(3, 30)
    map2.insert(1, 10)

    val map3 = createExternalMap[Int]
    map3.insert(3, 30)
    map3.insert(1, 10)
    map3.insert(2, 20)

    val it1 = map1.iterator
    val it2 = map2.iterator
    val it3 = map3.iterator

    var kv1 = it1.next()
    var kv2 = it2.next()
    var kv3 = it3.next()
    assert(kv1._1 === kv2._1 && kv2._1 === kv3._1)
    assert(kv1._2 === kv2._2 && kv2._2 === kv3._2)

    kv1 = it1.next()
    kv2 = it2.next()
    kv3 = it3.next()
    assert(kv1._1 === kv2._1 && kv2._1 === kv3._1)
    assert(kv1._2 === kv2._2 && kv2._2 === kv3._2)

    kv1 = it1.next()
    kv2 = it2.next()
    kv3 = it3.next()
    assert(kv1._1 === kv2._1 && kv2._1 === kv3._1)
    assert(kv1._2 === kv2._2 && kv2._2 === kv3._2)
    sc.stop()
  }

  test("null keys and values") {//空键和值
    val conf = createSparkConf(loadDefaults = false)
    sc = new SparkContext("local", "test", conf)

    val map = createExternalMap[Int]
    map.insert(1, 5)
    map.insert(2, 6)
    map.insert(3, 7)
    assert(map.size === 3)
    assert(map.iterator.toSet === Set[(Int, Seq[Int])](
      (1, Seq[Int](5)),
      (2, Seq[Int](6)),
      (3, Seq[Int](7))
    ))

    // Null keys
    //空键
    val nullInt = null.asInstanceOf[Int]
    map.insert(nullInt, 8)
    assert(map.size === 4)
    assert(map.iterator.toSet === Set[(Int, Seq[Int])](
      (1, Seq[Int](5)),
      (2, Seq[Int](6)),
      (3, Seq[Int](7)),
      (nullInt, Seq[Int](8))
    ))

    // Null values
    //空值
    map.insert(4, nullInt)
    map.insert(nullInt, nullInt)
    assert(map.size === 5)
    val result = map.iterator.toSet[(Int, ArrayBuffer[Int])].map(kv => (kv._1, kv._2.toSet))
    assert(result === Set[(Int, Set[Int])](
      (1, Set[Int](5)),
      (2, Set[Int](6)),
      (3, Set[Int](7)),
      (4, Set[Int](nullInt)),
      (nullInt, Set[Int](nullInt, 8))
    ))
    sc.stop()
  }

  test("simple aggregator") {//简单的聚合
    val conf = createSparkConf(loadDefaults = false)
    sc = new SparkContext("local", "test", conf)

    // reduceByKey
    val rdd = sc.parallelize(1 to 10).map(i => (i%2, 1))
    val result1 = rdd.reduceByKey(_ + _).collect()
    assert(result1.toSet === Set[(Int, Int)]((0, 5), (1, 5)))

    // groupByKey
    val result2 = rdd.groupByKey().collect().map(x => (x._1, x._2.toList)).toSet
    assert(result2.toSet === Set[(Int, Seq[Int])]
      ((0, List[Int](1, 1, 1, 1, 1)), (1, List[Int](1, 1, 1, 1, 1))))
    sc.stop()
  }

  test("simple cogroup") {//简单群
    val conf = createSparkConf(loadDefaults = false)
    sc = new SparkContext("local", "test", conf)
    val rdd1 = sc.parallelize(1 to 4).map(i => (i, i))
    val rdd2 = sc.parallelize(1 to 4).map(i => (i%2, i))
    val result = rdd1.cogroup(rdd2).collect()

    result.foreach { case (i, (seq1, seq2)) =>
      i match {
        case 0 => assert(seq1.toSet === Set[Int]() && seq2.toSet === Set[Int](2, 4))
        case 1 => assert(seq1.toSet === Set[Int](1) && seq2.toSet === Set[Int](1, 3))
        case 2 => assert(seq1.toSet === Set[Int](2) && seq2.toSet === Set[Int]())
        case 3 => assert(seq1.toSet === Set[Int](3) && seq2.toSet === Set[Int]())
        case 4 => assert(seq1.toSet === Set[Int](4) && seq2.toSet === Set[Int]())
      }
    }
    sc.stop()
  }

  test("spilling") {//溢出
    testSimpleSpilling()
  }

  test("spilling with compression") {//举出压缩
    // Keep track of which compression codec we're using to report in test failure messages
    //跟踪我们正在使用的压缩编解码器在测试失败消息报告
    var lastCompressionCodec: Option[String] = None
    try {
      allCompressionCodecs.foreach { c =>
        lastCompressionCodec = Some(c)
        testSimpleSpilling(Some(c))
      }
    } catch {
      // Include compression codec used in test failure message
      // We need to catch Throwable here because assertion failures are not covered by Exceptions
      case t: Throwable =>
        val compressionMessage = lastCompressionCodec
          .map { c => "with compression using codec " + c }
          .getOrElse("without compression")
        val newException = new Exception(s"Test failed $compressionMessage:\n\n${t.getMessage}")
        newException.setStackTrace(t.getStackTrace)
        throw newException
    }
  }

  /**
   * Test spilling through simple aggregations and cogroups.
    * 通过简单的聚合和共同组合测试溢出
   * If a compression codec is provided, use it. Otherwise, do not compress spills.
    * 如果提供压缩编解码器,请使用它。否则,不要压缩泄漏
   */
  private def testSimpleSpilling(codec: Option[String] = None): Unit = {
    val conf = createSparkConf(loadDefaults = true, codec)  // Load defaults for Spark home
    //Shuffle过程中使用的内存达到总内存多少比例的时候开始Spill(临时写入外部存储或一直使用内存)
    conf.set("spark.shuffle.memoryFraction", "0.001")
    //sc = new SparkContext("local-cluster[1,1,1024]", "test", conf)
    sc = new SparkContext("local[*]", "test", conf)

    // reduceByKey - should spill ~8 times
    val rddA = sc.parallelize(0 until 100000).map(i => (i/2, i))
    val resultA = rddA.reduceByKey(math.max).collect()
    assert(resultA.length === 50000)
    resultA.foreach { case (k, v) =>
      assert(v === k * 2 + 1, s"Value for $k was wrong: expected ${k * 2 + 1}, got $v")
    }

    // groupByKey - should spill ~17 times
    val rddB = sc.parallelize(0 until 100000).map(i => (i/4, i))
    val resultB = rddB.groupByKey().collect()
    assert(resultB.length === 25000)
    resultB.foreach { case (i, seq) =>
      val expected = Set(i * 4, i * 4 + 1, i * 4 + 2, i * 4 + 3)
      assert(seq.toSet === expected,
        s"Value for $i was wrong: expected $expected, got ${seq.toSet}")
    }

    // cogroup - should spill ~7 times
    val rddC1 = sc.parallelize(0 until 10000).map(i => (i, i))
    val rddC2 = sc.parallelize(0 until 10000).map(i => (i%1000, i))
    val resultC = rddC1.cogroup(rddC2).collect()
    assert(resultC.length === 10000)
    resultC.foreach { case (i, (seq1, seq2)) =>
      i match {
        case 0 =>
          assert(seq1.toSet === Set[Int](0))
          assert(seq2.toSet === Set[Int](0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000))
        case 1 =>
          assert(seq1.toSet === Set[Int](1))
          assert(seq2.toSet === Set[Int](1, 1001, 2001, 3001, 4001, 5001, 6001, 7001, 8001, 9001))
        case 5000 =>
          assert(seq1.toSet === Set[Int](5000))
          assert(seq2.toSet === Set[Int]())
        case 9999 =>
          assert(seq1.toSet === Set[Int](9999))
          assert(seq2.toSet === Set[Int]())
        case _ =>
      }
    }
    sc.stop()
  }

  test("spilling with hash collisions") {
    val conf = createSparkConf(loadDefaults = true)
    //Shuffle过程中使用的内存达到总内存多少比例的时候开始Spill(临时写入外部存储或一直使用内存)
    conf.set("spark.shuffle.memoryFraction", "0.001")
 //   sc = new SparkContext("local-cluster[1,1,1024]", "test", conf)
    sc = new SparkContext("local[*]", "test", conf)
    val map = createExternalMap[String]

    val collisionPairs = Seq(
      ("Aa", "BB"),                   // 2112
      ("to", "v1"),                   // 3707
      ("variants", "gelato"),         // -1249574770
      ("Teheran", "Siblings"),        // 231609873
      ("misused", "horsemints"),      // 1069518484
      ("isohel", "epistolaries"),     // -1179291542
      ("righto", "buzzards"),         // -931102253
      ("hierarch", "crinolines"),     // -1732884796
      ("inwork", "hypercatalexes"),   // -1183663690
      ("wainages", "presentencing"),  // 240183619
      ("trichothecenes", "locular"),  // 339006536
      ("pomatoes", "eructation")      // 568647356
    )

    collisionPairs.foreach { case (w1, w2) =>
      // String.hashCode is documented to use a specific algorithm, but check just in case
      assert(w1.hashCode === w2.hashCode)
    }

    map.insertAll((1 to 100000).iterator.map(_.toString).map(i => (i, i)))
    collisionPairs.foreach { case (w1, w2) =>
      map.insert(w1, w2)
      map.insert(w2, w1)
    }

    // A map of collision pairs in both directions
    val collisionPairsMap = (collisionPairs ++ collisionPairs.map(_.swap)).toMap

    // Avoid map.size or map.iterator.length because this destructively sorts the underlying map
    //避免使用map.size或map.iterator.length,因为这是破坏性排序底层的地图
    var count = 0

    val it = map.iterator
    while (it.hasNext) {
      val kv = it.next()
      val expectedValue = ArrayBuffer[String](collisionPairsMap.getOrElse(kv._1, kv._1))
      assert(kv._2.equals(expectedValue))
      count += 1
    }
    assert(count === 100000 + collisionPairs.size * 2)
    sc.stop()
  }
  //溢出许多哈希冲突
  test("spilling with many hash collisions") {
    val conf = createSparkConf(loadDefaults = true)
    //Shuffle过程中使用的内存达到总内存多少比例的时候开始Spill(临时写入外部存储或一直使用内存)
    conf.set("spark.shuffle.memoryFraction", "0.0001")
    //sc = new SparkContext("local-cluster[1,1,1024]", "test", conf)
    sc = new SparkContext("local[*]", "test", conf)
    val map = new ExternalAppendOnlyMap[FixedHashObject, Int, Int](_ => 1, _ + _, _ + _)

    // Insert 10 copies each of lots of objects whose hash codes are either 0 or 1. This causes
    // problems if the map fails to group together the objects with the same code (SPARK-2043).
    //插入10个副本,每个对象的散列代码为0或1,这导致如果Map无法使用相同代码将对象分组在一起(SPARK-2043)的问题。
    for (i <- 1 to 10) {
      for (j <- 1 to 10000) {
        map.insert(FixedHashObject(j, j % 2), 1)
      }
    }

    val it = map.iterator
    var count = 0
    while (it.hasNext) {
      val kv = it.next()
      assert(kv._2 === 10)
      count += 1
    }
    assert(count === 10000)
    sc.stop()
  }
  //使用Int.MaxValue键溢出哈希冲突
  test("spilling with hash collisions using the Int.MaxValue key") {
    val conf = createSparkConf(loadDefaults = true)
    //Shuffle过程中使用的内存达到总内存多少比例的时候开始Spill(临时写入外部存储或一直使用内存)
    conf.set("spark.shuffle.memoryFraction", "0.001")
    //sc = new SparkContext("local-cluster[1,1,1024]", "test", conf)
    sc = new SparkContext("local[*]", "test", conf)
    val map = createExternalMap[Int]

    (1 to 100000).foreach { i => map.insert(i, i) }
    map.insert(Int.MaxValue, Int.MaxValue)

    val it = map.iterator
    while (it.hasNext) {
      // Should not throw NoSuchElementException
      it.next()
    }
    sc.stop()
  }

  test("spilling with null keys and values") {//溢出空键和值
    val conf = createSparkConf(loadDefaults = true)
    //Shuffle过程中使用的内存达到总内存多少比例的时候开始Spill(临时写入外部存储或一直使用内存)
    conf.set("spark.shuffle.memoryFraction", "0.001")
    //sc = new SparkContext("local-cluster[1,1,1024]", "test", conf)
    sc = new SparkContext("local[*]", "test", conf)
    val map = createExternalMap[Int]

    map.insertAll((1 to 100000).iterator.map(i => (i, i)))
    map.insert(null.asInstanceOf[Int], 1)
    map.insert(1, null.asInstanceOf[Int])
    map.insert(null.asInstanceOf[Int], null.asInstanceOf[Int])

    val it = map.iterator
    while (it.hasNext) {
      // Should not throw NullPointerException
      it.next()
    }
    sc.stop()
  }

  test("external aggregation updates peak execution memory") {//外部聚合更新执行内存值
    val conf = createSparkConf(loadDefaults = false)
      //memoryFraction Shuffle过程中使用的内存达到总内存多少比例的时候开始Spill(临时写入外部存储或一直使用内存)
      .set("spark.shuffle.memoryFraction", "0.001")
      .set("spark.shuffle.manager", "hash") // make sure we're not also using ExternalSorter
    sc = new SparkContext("local", "test", conf)
    // No spilling,没有溢出
   /* AccumulatorSuite.verifyPeakExecutionMemorySet(sc, "external map without spilling") {
      sc.parallelize(1 to 10, 2).map { i => (i, i) }.reduceByKey(_ + _).count()
    }
    // With spilling 溢出
    AccumulatorSuite.verifyPeakExecutionMemorySet(sc, "external map with spilling") {
      sc.parallelize(1 to 1000 * 1000, 2).map { i => (i, i) }.reduceByKey(_ + _).count()
    }*/
  }

}
