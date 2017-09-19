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

import org.apache.spark.{SharedSparkContext, SparkFunSuite}
import org.apache.spark.util.random.{BernoulliSampler, PoissonSampler, RandomSampler}

/** 
 *  a sampler that outputs its seed
 *  一个简单输出种子 
 *  */
class MockSampler extends RandomSampler[Long, Long] {

  private var s: Long = _

  override def setSeed(seed: Long) {
    s = seed
  }

  override def sample(items: Iterator[Long]): Iterator[Long] = {
    Iterator(s)
  }

  override def clone: MockSampler = new MockSampler
}

class PartitionwiseSampledRDDSuite extends SparkFunSuite with SharedSparkContext {

  test("seed distribution") {//分布式种子
    val rdd = sc.makeRDD(Array(1L, 2L, 3L, 4L), 2)
    val sampler = new MockSampler
    val sample = new PartitionwiseSampledRDD[Long, Long](rdd, sampler, false, 0L)
    assert(sample.distinct().count == 2, "Seeds must be different.")
  }

  test("concurrency") {//并发
    // SPARK-2251: zip with self computes each partition twice.
    //用自己计算每个分区的两倍
    // We want to make sure there are no concurrency issues.
    //我们要确保没有并发问题
    val rdd = sc.parallelize(0 until 111, 10)
    for (sampler <- Seq(new BernoulliSampler[Int](0.5), new PoissonSampler[Int](0.5))) {
      val sampled = new PartitionwiseSampledRDD[Int, Int](rdd, sampler, true)
      sampled.zip(sampled).count()
    }
  }
}

