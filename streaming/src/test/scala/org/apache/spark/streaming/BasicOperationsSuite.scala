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

package org.apache.spark.streaming

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, SynchronizedBuffer}
import scala.language.existentials
import scala.reflect.ClassTag

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.{BlockRDD, RDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, WindowedDStream}
import org.apache.spark.util.{Clock, ManualClock}
import org.apache.spark.HashPartitioner
//基本操作套件
class BasicOperationsSuite extends TestSuiteBase {
  test("map") {
    //input=List(Range(1, 2, 3, 4), Range(5, 6, 7, 8), Range(9, 10, 11, 12))
    val input = Seq(1 to 4, 5 to 8, 9 to 12)    
    testOperation(
      input,
      (r: DStream[Int]) => {
         r.map(p=>{
           println("------"+p)
           p.toString
          })
         },
      input.map(_.map(_.toString))
    )
  }

  test("flatMap") {
    //input=List(Range(1, 2, 3, 4), Range(5, 6, 7, 8), Range(9, 10, 11, 12))
    val input = Seq(1 to 4, 5 to 8, 9 to 12)
    testOperation(
      //输入列表
      input, 
      //输入列表的操作
      (r: DStream[Int]) => r.flatMap(x => Seq(x, x * 2)),
      //期望的值
      input.map(_.flatMap{x => 
              //println("flatMap:"+x * 2)
                Array(x, x * 2)
              })
    )
  }

  test("filter") {
    //input: Seq = List(Range(1, 2, 3, 4), Range(5, 6, 7, 8), Range(9, 10, 11, 12))
    val input = Seq(1 to 4, 5 to 8, 9 to 12)
    testOperation(
      input,
      //DStream[Int] 泛型
      (r: DStream[Int]) => r.filter(x => (x % 2 == 0)),
      input.map(_.filter(x => (x % 2 == 0)))
    )
  }

  test("glom") {
    assert(numInputPartitions === 2, "Number of input partitions has been changed from 2")
    val input = Seq(1 to 4, 5 to 8, 9 to 12)
    val output = Seq(
      Seq( Seq(1, 2), Seq(3, 4) ),
      Seq( Seq(5, 6), Seq(7, 8) ),
      Seq( Seq(9, 10), Seq(11, 12) )
    )
    //operation匿名函数,接收DStream[Int],返回DStream[Int]
    val operation = (r: DStream[Int]) => r.glom().map(_.toSeq)
    testOperation(input, operation, output)
  }

  test("mapPartitions") {
    assert(numInputPartitions === 2, "Number of input partitions has been changed from 2")
    val input = Seq(1 to 4, 5 to 8, 9 to 12)
    val output = Seq(Seq(3, 7), Seq(11, 15), Seq(19, 23))
    val operation = (r: DStream[Int]) => r.mapPartitions(x => Iterator(x.reduce(_ + _)))
    testOperation(input, operation, output, true)
  }

  test("repartition (more partitions)") {//重新分配
    val input = Seq(1 to 100, 101 to 200, 201 to 300)
    val operation = (r: DStream[Int]) => r.repartition(5)
    withStreamingContext(setupStreams(input, operation, 2)) { ssc =>
      val output = runStreamsWithPartitions(ssc, 3, 3)
      assert(output.size === 3)
      val first = output(0)
      val second = output(1)
      val third = output(2)

      assert(first.size === 5)
      assert(second.size === 5)
      assert(third.size === 5)

      assert(first.flatten.toSet.equals((1 to 100).toSet))
      assert(second.flatten.toSet.equals((101 to 200).toSet))
      assert(third.flatten.toSet.equals((201 to 300).toSet))
    }
  }

  test("repartition (fewer partitions)") {//重新分配
    val input = Seq(1 to 100, 101 to 200, 201 to 300)
    //operation匿名函数,接收DStream[Int],返回DStream[Int]
    val operation = (r: DStream[Int]) => r.repartition(2)
    withStreamingContext(setupStreams(input, operation, 5)) { ssc =>
      val output = runStreamsWithPartitions(ssc, 3, 3)
      assert(output.size === 3)
      val first = output(0)
      val second = output(1)
      val third = output(2)

      assert(first.size === 2)
      assert(second.size === 2)
      assert(third.size === 2)

      assert(first.flatten.toSet.equals((1 to 100).toSet))
      assert(second.flatten.toSet.equals((101 to 200).toSet))
      assert(third.flatten.toSet.equals((201 to 300).toSet))
    }
  }

  test("groupByKey") {//key分组
    testOperation(
      Seq( Seq("a", "a", "b"), Seq("", ""), Seq() ),
      (s: DStream[String]) => s.map(x => (x, 1)).groupByKey().mapValues(_.toSeq),
      Seq( Seq(("a", Seq(1, 1)), ("b", Seq(1))), Seq(("", Seq(1, 1))), Seq() ),
      true
    )
  }

  test("reduceByKey") {//key分组合计
    testOperation(
      Seq( Seq("a", "a", "b"), Seq("", ""), Seq() ),
      (s: DStream[String]) => s.map(x => (x, 1)).reduceByKey(_ + _),
      Seq( Seq(("a", 2), ("b", 1)), Seq(("", 2)), Seq() ),
      true
    )
  }

  test("reduce") {//计算求和
     //input=List(Range(1, 2, 3, 4), Range(5, 6, 7, 8), Range(9, 10, 11, 12))
    testOperation(
      Seq(1 to 4, 5 to 8, 9 to 12),
      (s: DStream[Int]) => s.reduce(_ + _),
      Seq(Seq(10), Seq(26), Seq(42))
    )
  }

  test("count") {//序列计数
    testOperation(
      Seq(Seq(), 1 to 1, 1 to 2, 1 to 3, 1 to 4),
      (s: DStream[Int]) => s.count(),
      Seq(Seq(0L), Seq(1L), Seq(2L), Seq(3L), Seq(4L))
    )
  }

  test("countByValue") {
    //res0: Seq[Seq[Int]] = List(Range(1), List(1, 1, 1), Range(1, 2), List(1, 1, 2, 2))
    testOperation(
      Seq(1 to 1, Seq(1, 1, 1), 1 to 2, Seq(1, 1, 2, 2)),
      (s: DStream[Int]) => s.countByValue(),
      //统计value值数(1, 1L), (2, 1L)2有1个值
      Seq(Seq((1, 1L)), Seq((1, 3L)), Seq((1, 1L), (2, 1L)), Seq((2, 2L), (1, 2L))),
      true
    )
  }

  test("mapValues") {
    //res1: Seq[Seq[String]] = List(List(a, a, b), List("", ""), List())
    testOperation(
      Seq( Seq("a", "a", "b"), Seq("", ""), Seq() ),
      //以key值分组求和,把值再加10
      (s: DStream[String]) => s.map(x => (x, 1)).reduceByKey(_ + _).mapValues(_ + 10),
      //a有两个加10=12
      Seq( Seq(("a", 12), ("b", 11)), Seq(("", 12)), Seq() ),
      true
    )
  }

  test("flatMapValues") {
    testOperation(
      //res2: Seq[Seq[String]] = List(List(a, a, b), List("", ""), List())
      Seq( Seq("a", "a", "b"), Seq("", ""), Seq() ),
      (s: DStream[String]) => {
        s.map(x => (x, 1)).reduceByKey(_ + _).flatMapValues(x => Seq(x, x + 10))
      },
      Seq( Seq(("a", 2), ("a", 12), ("b", 1), ("b", 11)), Seq(("", 2), ("", 12)), Seq() ),
      true
    )
  }

  test("union") {
    val input = Seq(1 to 4, 101 to 104, 201 to 204)
    val output = Seq(1 to 8, 101 to 108, 201 to 208)
    testOperation(
      input,
      (s: DStream[Int]) => s.union(s.map(_ + 4)) ,
      output
    )
  }

  test("StreamingContext.union") {
    val input = Seq(1 to 4, 101 to 104, 201 to 204)
    val output = Seq(1 to 12, 101 to 112, 201 to 212)
    // union over 3 DStreams
    // 联合覆盖3DStreams
    testOperation(        
      input,
      (s: DStream[Int]) => s.context.union(Seq(s, s.map(_ + 4), s.map(_ + 8))),
      output
    )
  }

  test("transform") {
    //input= List(Range(1, 2, 3, 4), Range(5, 6, 7, 8), Range(9, 10, 11, 12))
    val input = Seq(1 to 4, 5 to 8, 9 to 12)
    testOperation(
      input,
      (r: DStream[Int]) => r.transform(rdd => rdd.map(_.toString)),   // RDD.map in transform
      input.map(_.map(_.toString))
    )
  }

  test("transformWith") {
    val inputData1 = Seq( Seq("a", "b"), Seq("a", ""), Seq(""), Seq() )
    val inputData2 = Seq( Seq("a", "b"), Seq("b", ""), Seq(), Seq("")   )
    val outputData = Seq(
      Seq( ("a", (1, "x")), ("b", (1, "x")) ),
      Seq( ("", (1, "x")) ),
      Seq(  ),
      Seq(  )
    )
    val operation = (s1: DStream[String], s2: DStream[String]) => {
      val t1 = s1.map(x => (x, 1))
      val t2 = s2.map(x => (x, "x"))
      t1.transformWith(           // RDD.join in transform
        t2,
        (rdd1: RDD[(String, Int)], rdd2: RDD[(String, String)]) => rdd1.join(rdd2)
      )
    }
    testOperation(inputData1, inputData2, operation, outputData, true)
  }

  test("StreamingContext.transform") {
    val input = Seq(1 to 4, 101 to 104, 201 to 204)
    val output = Seq(1 to 12, 101 to 112, 201 to 212)

    // transform over 3 DStreams by doing union of the 3 RDDs
    val operation = (s: DStream[Int]) => {
      s.context.transform(
        Seq(s, s.map(_ + 4), s.map(_ + 8)),   // 3 DStreams
        (rdds: Seq[RDD[_]], time: Time) =>
          rdds.head.context.union(rdds.map(_.asInstanceOf[RDD[Int]]))  // union of RDDs
      )
    }

    testOperation(input, operation, output)
  }

  test("cogroup") {
    val inputData1 = Seq( Seq("a", "a", "b"), Seq("a", ""), Seq(""), Seq() )
    val inputData2 = Seq( Seq("a", "a", "b"), Seq("b", ""), Seq(), Seq()   )
    val outputData = Seq(
      Seq( ("a", (Seq(1, 1), Seq("x", "x"))), ("b", (Seq(1), Seq("x"))) ),
      Seq( ("a", (Seq(1), Seq())), ("b", (Seq(), Seq("x"))), ("", (Seq(1), Seq("x"))) ),
      Seq( ("", (Seq(1), Seq())) ),
      Seq(  )
    )
    val operation = (s1: DStream[String], s2: DStream[String]) => {
      s1.map(x => (x, 1)).cogroup(s2.map(x => (x, "x"))).mapValues(x => (x._1.toSeq, x._2.toSeq))
    }
    testOperation(inputData1, inputData2, operation, outputData, true)
  }

  test("join") {
    val inputData1 = Seq( Seq("a", "b"), Seq("a", ""), Seq(""), Seq() )
    val inputData2 = Seq( Seq("a", "b"), Seq("b", ""), Seq(), Seq("")   )
    val outputData = Seq(
      Seq( ("a", (1, "x")), ("b", (1, "x")) ),
      Seq( ("", (1, "x")) ),
      Seq(  ),
      Seq(  )
    )
    val operation = (s1: DStream[String], s2: DStream[String]) => {
      s1.map(x => (x, 1)).join(s2.map(x => (x, "x")))
    }
    testOperation(inputData1, inputData2, operation, outputData, true)
  }

  test("leftOuterJoin") {
    val inputData1 = Seq( Seq("a", "b"), Seq("a", ""), Seq(""), Seq() )
    val inputData2 = Seq( Seq("a", "b"), Seq("b", ""), Seq(), Seq("")   )
    val outputData = Seq(
      Seq( ("a", (1, Some("x"))), ("b", (1, Some("x"))) ),
      Seq( ("", (1, Some("x"))), ("a", (1, None)) ),
      Seq( ("", (1, None)) ),
      Seq(  )
    )
    val operation = (s1: DStream[String], s2: DStream[String]) => {
      s1.map(x => (x, 1)).leftOuterJoin(s2.map(x => (x, "x")))
    }
    testOperation(inputData1, inputData2, operation, outputData, true)
  }

 /* test("rightOuterJoin") {
    val inputData1 = Seq( Seq("a", "b"), Seq("a", ""), Seq(""), Seq() )
    val inputData2 = Seq( Seq("a", "b"), Seq("b", ""), Seq(), Seq("")   )
    val outputData = Seq(
      Seq( ("a", (Some(1), "x")), ("b", (Some(1), "x")) ),
      Seq( ("", (Some(1), "x")), ("b", (None, "x")) ),
      Seq(  ),
      Seq( ("", (None, "x")) )
    )
    val operation = (s1: DStream[String], s2: DStream[String]) => {
      s1.map(x => (x, 1)).rightOuterJoin(s2.map(x => (x, "x")))
    }
    testOperation(inputData1, inputData2, operation, outputData, true)
  }*/

  test("fullOuterJoin") {
    val inputData1 = Seq( Seq("a", "b"), Seq("a", ""), Seq(""), Seq() )
    val inputData2 = Seq( Seq("a", "b"), Seq("b", ""), Seq(), Seq("")   )
    val outputData = Seq(
      Seq( ("a", (Some(1), Some("x"))), ("b", (Some(1), Some("x"))) ),
      Seq( ("", (Some(1), Some("x"))), ("a", (Some(1), None)), ("b", (None, Some("x"))) ),
      Seq( ("", (Some(1), None)) ),
      Seq( ("", (None, Some("x"))) )
    )
    val operation = (s1: DStream[String], s2: DStream[String]) => {
      s1.map(x => (x, 1)).fullOuterJoin(s2.map(x => (x, "x")))
    }
    testOperation(inputData1, inputData2, operation, outputData, true)
  }
  //updateStateByKey可以DStream中的数据进行按key做reduce操作,然后对各个批次的数据进行累加
  /**
  *updateStateByKey 操作返回一个新状态的DStream,
  *其中传入的函数基于键之前的状态和键新的值更新每个键的状态
  *updateStateByKey操作对每个键会调用一次,
  *values表示键对应的值序列,state可以是任务状态
  */
  /*test("updateStateByKey") {
    //输入数据
    val inputData =
      Seq(
        Seq("a"),
        Seq("a", "b"),
        Seq("a", "b", "c"),
        Seq("a", "b"),
        Seq("a"),
        Seq()
      )
    //输出数据
    val outputData =
      Seq(
        Seq(("a", 1)),
        Seq(("a", 2), ("b", 1)),
        Seq(("a", 3), ("b", 2), ("c", 1)),
        Seq(("a", 4), ("b", 3), ("c", 1)),
        Seq(("a", 5), ("b", 3), ("c", 1)),
        Seq(("a", 5), ("b", 3), ("c", 1))
      )
    //updateStateOperation匿名函数,接收DStream[String]类型,返回DStream[(String, Int)]
  /**
  *updateStateByKey 操作返回一个新状态的DStream,
  *其中传入的函数基于键之前的状态和键新的值更新每个键的状态
  *updateStateByKey操作对每个键会调用一次,
  *values表示键对应的值序列,state可以是任务状态
  **/
    val updateStateOperation = (s: DStream[String]) => {
      val updateFunc = (values: Seq[Int], state: Option[Int]) => {      
        println("values:["+values.mkString(",") +"]\t state:["+ state.mkString(",")+"]")
        //getOrElse默认值为0
        Some(values.sum + state.getOrElse(0))
      }
       /**
       * updateStateByKey可以DStream中的数据进行按key做reduce操作,然后对各个批次的数据进行累加     
       * updateStateByKey在有新的数据信息进入或更新时,可以让用户保持想要的任何状       
       * 1)定义状态:可以是任意数据类型
       * 2)定义状态更新函数:用一个函数指定如何使用先前的状态,从输入流中的新值更新状态。
       * 对于有状态操作,要不断的把当前和历史的时间切片的RDD累加计算,随着时间的流失,计算的数据规模会变得越来越大。
       */
      s.map(x => (x, 1)).updateStateByKey[Int](updateFunc)
    }

    testOperation(inputData, updateStateOperation, outputData, true)
  }*/
  //updateStateByKey-简单的RDD初始化值
  test("updateStateByKey - simple with initial value RDD") {
    val initial = Seq(("a", 1), ("c", 2))

    val inputData =
      Seq(
        Seq("a"),
        Seq("a", "b"),
        Seq("a", "b", "c"),
        Seq("a", "b"),
        Seq("a"),
        Seq()
      )

    val outputData =
      Seq(
        Seq(("a", 2), ("c", 2)),
        Seq(("a", 3), ("b", 1), ("c", 2)),
        Seq(("a", 4), ("b", 2), ("c", 3)),
        Seq(("a", 5), ("b", 3), ("c", 3)),
        Seq(("a", 6), ("b", 3), ("c", 3)),
        Seq(("a", 6), ("b", 3), ("c", 3))
      )

    val updateStateOperation = (s: DStream[String]) => {
      val initialRDD = s.context.sparkContext.makeRDD(initial)
      val updateFunc = (values: Seq[Int], state: Option[Int]) => {
        Some(values.sum + state.getOrElse(0))
      }
      //初始化值
  /**
  *updateStateByKey 操作返回一个新状态的DStream,
  *其中传入的函数基于键之前的状态和键新的值更新每个键的状态
  *updateStateByKey操作对每个键会调用一次,
  *values表示键对应的值序列,state可以是任务状态
  **/
      s.map(x => (x, 1)).updateStateByKey[Int](updateFunc,
        new HashPartitioner (numInputPartitions), initialRDD)
    }

    testOperation(inputData, updateStateOperation, outputData, true)
  }
 //updateStateByKey-RDD初始化值
  test("updateStateByKey - with initial value RDD") {
    val initial = Seq(("a", 1), ("c", 2))

    val inputData =
      Seq(
        Seq("a"),
        Seq("a", "b"),
        Seq("a", "b", "c"),
        Seq("a", "b"),
        Seq("a"),
        Seq()
      )

    val outputData =
      Seq(
        Seq(("a", 2), ("c", 2)),
        Seq(("a", 3), ("b", 1), ("c", 2)),
        Seq(("a", 4), ("b", 2), ("c", 3)),
        Seq(("a", 5), ("b", 3), ("c", 3)),
        Seq(("a", 6), ("b", 3), ("c", 3)),
        Seq(("a", 6), ("b", 3), ("c", 3))
      )

    val updateStateOperation = (s: DStream[String]) => {
      val initialRDD = s.context.sparkContext.makeRDD(initial)
      val updateFunc = (values: Seq[Int], state: Option[Int]) => {
        Some(values.sum + state.getOrElse(0))
      }
      val newUpdateFunc = (iterator: Iterator[(String, Seq[Int], Option[Int])]) => {
        iterator.flatMap(t => updateFunc(t._2, t._3).map(s => (t._1, s)))
      }
/**
  *updateStateByKey 操作返回一个新状态的DStream,
  *其中传入的函数基于键之前的状态和键新的值更新每个键的状态
  *updateStateByKey操作对每个键会调用一次,
  *values表示键对应的值序列,state可以是任务状态
  **/

      s.map(x => (x, 1)).updateStateByKey[Int](newUpdateFunc,
        new HashPartitioner (numInputPartitions), true, initialRDD)
    }

    testOperation(inputData, updateStateOperation, outputData, true)
  }
  //updateStateByKey-对象的生命周期
  test("updateStateByKey - object lifecycle") {
    val inputData =
      Seq(
        Seq("a", "b"),
        null,
        Seq("a", "c", "a"),
        Seq("c"),
        null,
        null
      )

    val outputData =
      Seq(
        Seq(("a", 1), ("b", 1)),
        Seq(("a", 1), ("b", 1)),
        Seq(("a", 3), ("c", 1)),
        Seq(("a", 3), ("c", 2)),
        Seq(("c", 2)),
        Seq()
      )

    val updateStateOperation = (s: DStream[String]) => {
      class StateObject(var counter: Int = 0, var expireCounter: Int = 0) extends Serializable

      // updateFunc clears a state when a StateObject is seen without new values twice in a row
      val updateFunc = (values: Seq[Int], state: Option[StateObject]) => {
        val stateObj = state.getOrElse(new StateObject)
        values.sum match {
          case 0 => stateObj.expireCounter += 1 // no new values
          case n => { // has new values, increment and reset expireCounter
            stateObj.counter += n
            stateObj.expireCounter = 0
          }
        }
        stateObj.expireCounter match {
          //两次没有新的值,给它引导
          case 2 => None // seen twice with no new values, give it the boot
          case _ => Option(stateObj)
        }
      }
      /**
  *updateStateByKey 操作返回一个新状态的DStream,
  *其中传入的函数基于键之前的状态和键新的值更新每个键的状态
  *updateStateByKey操作对每个键会调用一次,
  *values表示键对应的值序列,state可以是任务状态
  **/
      s.map(x => (x, 1)).updateStateByKey[StateObject](updateFunc).mapValues(_.counter)
    }

    testOperation(inputData, updateStateOperation, outputData, true)
  }

  test("slice") {//返回从fromTime到toTime之间的所有RDD的序列
    //以1秒为单位输入流
    withStreamingContext(new StreamingContext(conf, Seconds(1))) { ssc =>
      //input=List(List(1), List(2), List(3), List(4))
      val input = Seq(Seq(1), Seq(2), Seq(3), Seq(4))
      val stream = new TestInputStream[Int](ssc, input, 2)
      //虚拟输出流,触发Dstrem计算功能
      stream.foreachRDD(_ => {})  // Dummy output stream
      ssc.start()
      //sleep方法用于将当前线程休眠一定 时间,单位是毫秒 1000毫秒
      Thread.sleep(2000)
      //获得输入流的切分,参数fromMillis开始毫秒,toMillis结束毫秒
      def getInputFromSlice(fromMillis: Long, toMillis: Long): Set[Int] = {
      //slice返回从fromTime到toTime之间的所有RDD的序列
        stream.slice(new Time(fromMillis), new Time(toMillis)).flatMap(_.collect()).toSet
      }

      assert(getInputFromSlice(0, 1000) == Set(1))
      assert(getInputFromSlice(0, 2000) == Set(1, 2))
      assert(getInputFromSlice(1000, 2000) == Set(1, 2))
      assert(getInputFromSlice(2000, 4000) == Set(2, 3, 4))
    }
  }
  test("slice - has not been initialized") {//slice -没有被初始化
    withStreamingContext(new StreamingContext(conf, Seconds(1))) { ssc =>
      val input = Seq(Seq(1), Seq(2), Seq(3), Seq(4))
      val stream = new TestInputStream[Int](ssc, input, 2)
      val thrown = intercept[SparkException] {
      //slice返回从fromTime到toTime之间的所有RDD的序列
        stream.slice(new Time(0), new Time(1000))
      }
      assert(thrown.getMessage.contains("has not been initialized"))
    }
  }
/**
 * res0: Vector(List(0, 1), List(1, 2),List(2, 3), List(3, 4), List(4, 5), 
 * 						List(5, 6), List(6, 7), List(7, 8), List(8,9), List(9, 10))
 */
  val cleanupTestInput = (0 until 10).map(x => Seq(x, x + 1)).toSeq

  test("rdd cleanup - map and window") {
    val rememberDuration = Seconds(3)
    def operation(s: DStream[Int]): DStream[(Int, Int)] = {
      s.map(x => (x % 10, 1))
      //返回一个包含了所有在时间滑动窗口中可见元素的新的DStream
       .window(Seconds(2), Seconds(1))
       .window(Seconds(4), Seconds(2))
    }

    val operatedStream = runCleanupTest(conf, operation _,
        // 5=cleanupTestInput.size / 2
      numExpectedOutput = cleanupTestInput.size / 2, rememberDuration = Seconds(3))
    val windowedStream2 = operatedStream.asInstanceOf[WindowedDStream[_]]
    val windowedStream1 = windowedStream2.dependencies.head.asInstanceOf[WindowedDStream[_]]
    val mappedStream = windowedStream1.dependencies.head

    // Checkpoint remember durations 记住时间点
    assert(windowedStream2.rememberDuration === rememberDuration)
    assert(windowedStream1.rememberDuration === rememberDuration + windowedStream2.windowDuration)
    assert(mappedStream.rememberDuration ===
      rememberDuration + windowedStream2.windowDuration + windowedStream1.windowDuration)

    // WindowedStream2 should remember till 7 seconds: 10, 9, 8, 7
    ///WindowedStream2 应该记住直到7秒:10, 9, 8, 7
    // WindowedStream1 should remember till 4 seconds: 10, 9, 8, 7, 6, 5, 4
    //WindowedStream1 应该记住直到4秒:10, 9, 8, 7, 6, 5, 4
    // MappedStream should remember till 2 seconds:    10, 9, 8, 7, 6, 5, 4, 3, 2

    // WindowedStream2
    assert(windowedStream2.generatedRDDs.contains(Time(10000)))
    assert(windowedStream2.generatedRDDs.contains(Time(8000)))
    assert(!windowedStream2.generatedRDDs.contains(Time(6000)))

    // WindowedStream1
    assert(windowedStream1.generatedRDDs.contains(Time(10000)))
    assert(windowedStream1.generatedRDDs.contains(Time(4000)))
    assert(!windowedStream1.generatedRDDs.contains(Time(3000)))

    // MappedStream
    assert(mappedStream.generatedRDDs.contains(Time(10000)))
    assert(mappedStream.generatedRDDs.contains(Time(2000)))
    assert(!mappedStream.generatedRDDs.contains(Time(1000)))
  }

  test("rdd cleanup - updateStateByKey") {
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      Some(values.sum + state.getOrElse(0))
    }
    val stateStream = runCleanupTest(
	//在interval周期后给生成的RDD设置检查点
  /**
  *updateStateByKey 操作返回一个新状态的DStream,
  *其中传入的函数基于键之前的状态和键新的值更新每个键的状态
  *updateStateByKey操作对每个键会调用一次,
  *values表示键对应的值序列,state可以是任务状态
  **/
      conf, _.map(_ -> 1).updateStateByKey(updateFunc).checkpoint(Seconds(3)))

    assert(stateStream.rememberDuration === stateStream.checkpointDuration * 2)
    assert(stateStream.generatedRDDs.contains(Time(10000)))
    assert(!stateStream.generatedRDDs.contains(Time(4000)))
  }
  
  test("rdd cleanup - input blocks and persisted RDDs") {
    // Actually receive data over through receiver to create BlockRDDs
    //其实接收数据通过接收器创建blockrdds
    withTestServer(new TestServer()) { testServer =>
      withStreamingContext(new StreamingContext(conf, batchDuration)) { ssc =>
        testServer.start()

        val batchCounter = new BatchCounter(ssc)

        // Set up the streaming context and input streams
        //设置流上下文和输入流
        val networkStream =
          ssc.socketTextStream("localhost", testServer.port, StorageLevel.MEMORY_AND_DISK)
        val mappedStream = networkStream.map(_ + ".").persist()
        val outputBuffer = new ArrayBuffer[Seq[String]] with SynchronizedBuffer[Seq[String]]
        val outputStream = new TestOutputStream(mappedStream, outputBuffer)
	 //register将当前DStream注册到DStreamGraph的输出流中
        outputStream.register()
        ssc.start()

        // Feed data to the server to send to the network receiver
        //将数据发送到服务器发送到网络接收器
        val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
        val input = Seq(1, 2, 3, 4, 5, 6)

        val blockRdds = new mutable.HashMap[Time, BlockRDD[_]]
        val persistentRddIds = new mutable.HashMap[Time, Int]
        //得到所有RDD信息进行核查的要求
        def collectRddInfo() { // get all RDD info required for verification
          networkStream.generatedRDDs.foreach { case (time, rdd) =>
            blockRdds(time) = rdd.asInstanceOf[BlockRDD[_]]
          }
          mappedStream.generatedRDDs.foreach { case (time, rdd) =>
            persistentRddIds(time) = rdd.id
          }
        }

        Thread.sleep(200)
        for (i <- 0 until input.size) {
          testServer.send(input(i).toString + "\n")
          Thread.sleep(200)
          val numCompletedBatches = batchCounter.getNumCompletedBatches
          clock.advance(batchDuration.milliseconds)
          if (!batchCounter.waitUntilBatchesCompleted(numCompletedBatches + 1, 5000)) {
            fail("Batch took more than 5 seconds to complete")
          }
          collectRddInfo()
        }

        Thread.sleep(200)
        collectRddInfo()
        logInfo("Stopping server")
        testServer.stop()

        // verify data has been received
        //已接收验证数据
        assert(outputBuffer.size > 0)
        assert(blockRdds.size > 0)
        assert(persistentRddIds.size > 0)

        import Time._

        val latestPersistedRddId = persistentRddIds(persistentRddIds.keySet.max)
        val earliestPersistedRddId = persistentRddIds(persistentRddIds.keySet.min)
        val latestBlockRdd = blockRdds(blockRdds.keySet.max)
        val earliestBlockRdd = blockRdds(blockRdds.keySet.min)
        // verify that the latest mapped RDD is persisted but the earliest one has been unpersisted
        //验证最新Map RDD持久化,但最早的一个没有持久化
        assert(ssc.sparkContext.persistentRdds.contains(latestPersistedRddId))
        assert(!ssc.sparkContext.persistentRdds.contains(earliestPersistedRddId))

        // verify that the latest input blocks are present but the earliest blocks have been removed
        //验证最新的输入块是否存在,但最早的块已被移除.
        assert(latestBlockRdd.isValid)
        assert(latestBlockRdd.collect != null)
        assert(!earliestBlockRdd.isValid)
        earliestBlockRdd.blockIds.foreach { blockId =>
          assert(!ssc.sparkContext.env.blockManager.master.contains(blockId))
        }
      }
    }
  }

  /** 
   *  Test cleanup of RDDs in DStream metadata
   *  测试清理RDDS dstream元数据
   *  */
  def runCleanupTest[T: ClassTag](
      conf2: SparkConf,
      operation: DStream[Int] => DStream[T],
      numExpectedOutput: Int = cleanupTestInput.size,
      rememberDuration: Duration = null
    ): DStream[T] = {

    // Setup the stream computation
    //设置流式计算
    assert(batchDuration === Seconds(1),
      //批量持续时间已从1秒改变,检查清理测试
      "Batch duration has changed from 1 second, check cleanup tests")
    withStreamingContext(setupStreams(cleanupTestInput, operation)) { ssc =>
      val operatedStream =
        ssc.graph.getOutputStreams().head.dependencies.head.asInstanceOf[DStream[T]]
      if (rememberDuration != null) ssc.remember(rememberDuration)
      val output = runStreams[(Int, Int)](ssc, cleanupTestInput.size, numExpectedOutput)
      val clock = ssc.scheduler.clock.asInstanceOf[Clock]
      assert(clock.getTimeMillis() === Seconds(10).milliseconds)
      assert(output.size === numExpectedOutput)
      operatedStream
    }
  }
}
