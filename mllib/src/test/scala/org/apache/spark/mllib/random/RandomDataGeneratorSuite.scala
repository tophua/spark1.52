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

package org.apache.spark.mllib.random

import scala.math

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.StatCounter

// TODO update tests to use TestingUtils for floating point comparison after PR 1367 is merged
//更新测试使用的testingutils浮点比较后PR 1367合并
class RandomDataGeneratorSuite extends SparkFunSuite {
/**
 * 随机数据生成在随机算法、原型开发、性能测试中比较有用
 */
  def apiChecks(gen: RandomDataGenerator[Double]) {
    // resetting seed should generate the same sequence of random numbers
    //重置种子应产生同一序列的随机数
    gen.setSeed(42L)
    val array1 = (0 until 1000).map(_ => gen.nextValue())
    gen.setSeed(42L)
    val array2 = (0 until 1000).map(_ => gen.nextValue())
    assert(array1.equals(array2))

    // newInstance should contain a difference instance of the rng
    // i.e. setting difference seeds for difference instances produces different sequences of
    // random numbers.
    val gen2 = gen.copy()
    gen.setSeed(0L)
    val array3 = (0 until 1000).map(_ => gen.nextValue())
    gen2.setSeed(1L)
    val array4 = (0 until 1000).map(_ => gen2.nextValue())
    // Compare arrays instead of elements since individual elements can coincide by chance but the
    // sequences should differ given two different seeds.
    //比较数组,而不是元素,因为单个元素可以在偶然的机会,但序列应该不同给定的两个不同的种子。
    assert(!array3.equals(array4))

    // test that setting the same seed in the copied instance produces the same sequence of numbers
    //测试在复制的实例中设置相同的种子产生相同的数字序列
    gen.setSeed(0L)
    val array5 = (0 until 1000).map(_ => gen.nextValue())
    gen2.setSeed(0L)
    val array6 = (0 until 1000).map(_ => gen2.nextValue())
    assert(array5.equals(array6))
  }

  def distributionChecks(gen: RandomDataGenerator[Double],
      mean: Double = 0.0,
      stddev: Double = 1.0,
      epsilon: Double = 0.01) {
    for (seed <- 0 until 5) {
      gen.setSeed(seed.toLong)
      val sample = (0 until 100000).map { _ => gen.nextValue()}
      val stats = new StatCounter(sample)
       //math.abs返回数的绝对值
      assert(math.abs(stats.mean - mean) < epsilon)
      assert(math.abs(stats.stdev - stddev) < epsilon)
    }
  }

  test("UniformGenerator") {//均匀分布随机数生成器
    val uniform = new UniformGenerator()
    apiChecks(uniform)
    //均匀分布的标准差= (ub - lb) / math.sqrt(12)
    // Stddev of uniform distribution = (ub - lb) / math.sqrt(12)
    //math.sqrt返回数字的平方根
    distributionChecks(uniform, 0.5, 1 / math.sqrt(12))
  }

  test("StandardNormalGenerator") {//标准正态生成器
    val normal = new StandardNormalGenerator()
    apiChecks(normal)
    distributionChecks(normal, 0.0, 1.0)
  }

  test("LogNormalGenerator") {//对数正态生成器
    List((0.0, 1.0), (0.0, 2.0), (2.0, 1.0), (2.0, 2.0)).map {
      case (mean: Double, vari: Double) =>
      //math.sqrt返回数字的平方根
        val normal = new LogNormalGenerator(mean, math.sqrt(vari))
        apiChecks(normal)

        // mean of log normal = e^(mean + var / 2)
        //对数正态平均= e^(mean + var / 2)
        val expectedMean = math.exp(mean + 0.5 * vari)
	
        // variance of log normal = (e^var - 1) * e^(2 * mean + var)
        //对数正态方差=(e^var - 1) * e^(2 * mean + var)
        val expectedStd = math.sqrt((math.exp(vari) - 1.0) * math.exp(2.0 * mean + vari))

        // since sampling error increases with variance, let's set
        //由于采样误差随方差增大,让我们把绝对公差定为百分比
        // the absolute tolerance as a percentage
        val epsilon = 0.05 * expectedStd * expectedStd

        distributionChecks(normal, expectedMean, expectedStd, epsilon)
    }
  }

  test("PoissonGenerator") {//泊松生成器
    // mean = 0.0 will not pass the API checks since 0.0 is always deterministically produced.
    //平均数=0.0 将不能通过API检查自0以来一直是确定性的产生
    for (mean <- List(1.0, 5.0, 100.0)) {
      val poisson = new PoissonGenerator(mean)
      apiChecks(poisson)
       //math.sqrt返回数字的平方根
      distributionChecks(poisson, mean, math.sqrt(mean), 0.1)
    }
  }

  test("ExponentialGenerator") {//指数函数生成器
    // mean = 0.0 will not pass the API checks since 0.0 is always deterministically produced.
    //平均数=0.0  将不能通过API检查自0以来一直是确定性的产生
    for (mean <- List(2.0, 5.0, 10.0, 50.0, 100.0)) {
      val exponential = new ExponentialGenerator(mean)
      apiChecks(exponential)
      // var of exp = lambda^-2 = (1.0 / mean)^-2 = mean^2

      // since sampling error increases with variance, let's set
      //由于采样误差随方差增大,让我们把绝对公差定为百分比
      // the absolute tolerance as a percentage
      val epsilon = 0.05 * mean * mean

      distributionChecks(exponential, mean, mean, epsilon)
    }
  }

  test("GammaGenerator") {//伽玛生成器
    // mean = 0.0 will not pass the API checks since 0.0 is always deterministically produced.
     //平均数=0.0  将不能通过API检查自0以来一直是确定性的产生
    List((1.0, 2.0), (2.0, 2.0), (3.0, 2.0), (5.0, 1.0), (9.0, 0.5)).map {
      case (shape: Double, scale: Double) =>
        val gamma = new GammaGenerator(shape, scale)
        apiChecks(gamma)
        // mean of gamma = shape * scale
        val expectedMean = shape * scale
        // var of gamma = shape * scale^2
	 //math.sqrt返回数字的平方根
        val expectedStd = math.sqrt(shape * scale * scale)
        distributionChecks(gamma, expectedMean, expectedStd, 0.1)
    }
  }
}
