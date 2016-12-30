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

package org.apache.spark.mllib.stat

import java.util.Random

import org.apache.commons.math3.distribution.{
  ExponentialDistribution,
  NormalDistribution,
  UniformRealDistribution
}
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest

import org.apache.spark.{ SparkException, SparkFunSuite }
import org.apache.spark.mllib.linalg.{ DenseVector, Matrices, Vectors }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.test.ChiSqTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
/**
 * 假设检验是鉴定给定的假说正确性概率的一种方式
 */
class HypothesisTestSuite extends SparkFunSuite with MLlibTestSparkContext {
  //皮尔森(goodness)适配度检测
  /**
   * 皮尔森适配度检测，Goodness of Fit test，验证一组观察值的次数分配是否异于理论上的分配。
   * 目前Spark默认的是均匀分配.
   */
  test("chi squared pearson goodness of fit") {
    //配度检测:验证一组观察值的次数分配是否异于理论上的分配
    val observed = new DenseVector(Array[Double](4, 6, 5))
    //提供了进行Pearson(皮尔森)卡方检验的方法
    val pearson = Statistics.chiSqTest(observed)//卡方检验   
    // Results validated against the R command `chisq.test(c(4, 6, 5), p=c(1/3, 1/3, 1/3))',P值一种概率
    //计算卡方检验的统计值(适配度检测):把每一个观察值和理论值(适配度检测,平均值)的差做平方后、除以理论值、再加总
    println("statistic:"+pearson.statistic+"\t degreesOfFreedom:"+pearson.degreesOfFreedom+"\t pValue:"+pearson.pValue)
    //statistic:0.4	 degreesOfFreedom:2	 pValue:0.8187307530779818
    assert(pearson.statistic === 0.4)
    //自由度为2
    assert(pearson.degreesOfFreedom === 2)
    //P 值，一般以P < 0.05 为显著， P <0.01 为非常显著，其含义是样本间的差异由抽样误差所致的概率小于0.05 或0.01
    assert(pearson.pValue ~== 0.8187 relTol 1e-4)//假定值、假设机率,由于总是介于0和1之间
    //名称
    assert(pearson.method === ChiSqTest.PEARSON.name)
    
    assert(pearson.nullHypothesis === ChiSqTest.NullHypothesis.goodnessOfFit.toString)
    //独立性检测:验证从两个变量抽出的配对观察值组是否互相独立
    // different expected and observed sum
    val observed1 = new DenseVector(Array[Double](21, 38, 43, 80))
    val expected1 = new DenseVector(Array[Double](3, 5, 7, 20))
    //提供了进行Pearson卡方检验的方法
    val pearson1 = Statistics.chiSqTest(observed1, expected1)

    // Results validated against the R command
    //对R命令进行验证的结果
    // `chisq.test(c(21, 38, 43, 80), p=c(3/35, 1/7, 1/5, 4/7))`
    //
    assert(pearson1.statistic ~== 14.1429 relTol 1e-4)
    assert(pearson1.degreesOfFreedom === 3)
    assert(pearson1.pValue ~== 0.002717 relTol 1e-4)
    assert(pearson1.method === ChiSqTest.PEARSON.name)
    assert(pearson1.nullHypothesis === ChiSqTest.NullHypothesis.goodnessOfFit.toString)

    // Vectors with different sizes
    //大小不同的向量
    val observed3 = new DenseVector(Array(1.0, 2.0, 3.0))
    val expected3 = new DenseVector(Array(1.0, 2.0, 3.0, 4.0))
    //提供了进行Pearson卡方检验的方法
    intercept[IllegalArgumentException](Statistics.chiSqTest(observed3, expected3))

    // negative counts in observed 观察中的负计数
    val negObs = new DenseVector(Array(1.0, 2.0, 3.0, -4.0))
    //提供了进行Pearson卡方检验的方法
    intercept[IllegalArgumentException](Statistics.chiSqTest(negObs, expected1))

    // count = 0.0 in expected but not observed 
    //计数= 0,在预期,但没有观察到
    val zeroExpected = new DenseVector(Array(1.0, 0.0, 3.0))
    //提供了进行Pearson卡方检验的方法
    val inf = Statistics.chiSqTest(observed, zeroExpected)
    assert(inf.statistic === Double.PositiveInfinity)
    assert(inf.degreesOfFreedom === 2)
    assert(inf.pValue === 0.0)
    assert(inf.method === ChiSqTest.PEARSON.name)
    assert(inf.nullHypothesis === ChiSqTest.NullHypothesis.goodnessOfFit.toString)

    // 0.0 in expected and observed simultaneously
    //0在预期和观察同时
    val zeroObserved = new DenseVector(Array(2.0, 0.0, 1.0))
    //提供了进行Pearson卡方检验的方法
    intercept[IllegalArgumentException](Statistics.chiSqTest(zeroObserved, zeroExpected))
  }
  /**皮尔森独立性(independence)检测**/
  /**
   * 独立性检测，independence test，验证从两个变量抽出的配对观察值组是否互相独立。
   * 其虚无假设是：两个变量呈统计独立性。
   */
  test("chi squared pearson matrix independence") {//卡方皮尔森矩阵独立性
    val data = Array(40.0, 24.0, 29.0, 56.0, 32.0, 42.0, 31.0, 10.0, 0.0, 30.0, 15.0, 12.0)
    // [[40.0, 56.0, 31.0, 30.0],
    //  [24.0, 32.0, 10.0, 15.0],
    //  [29.0, 42.0, 0.0,  12.0]]
    //提供了进行Pearson卡方检验的方法
    val chi = Statistics.chiSqTest(Matrices.dense(3, 4, data))
    //
    // Results validated against R command
    // `chisq.test(rbind(c(40, 56, 31, 30),c(24, 32, 10, 15), c(29, 42, 0, 12)))`
    
    assert(chi.statistic ~== 21.9958 relTol 1e-4)
    //自由度 df=(r−1)(c−1)=(3−1)∗(4−1)=6 //其中r为行数，c为列数
    assert(chi.degreesOfFreedom === 6)
    assert(chi.pValue ~== 0.001213 relTol 1e-4)//概率值
    assert(chi.method === ChiSqTest.PEARSON.name)
    assert(chi.nullHypothesis === ChiSqTest.NullHypothesis.independence.toString)

    // Negative counts 负计数
    val negCounts = Array(4.0, 5.0, 3.0, -3.0)
    //提供了进行Pearson卡方检验的方法
    intercept[IllegalArgumentException](Statistics.chiSqTest(Matrices.dense(2, 2, negCounts)))

    // Row sum = 0.0 行和
    val rowZero = Array(0.0, 1.0, 0.0, 2.0)
    //提供了进行Pearson卡方检验的方法
    intercept[IllegalArgumentException](Statistics.chiSqTest(Matrices.dense(2, 2, rowZero)))

    // Column sum  = 0.0 列和
    val colZero = Array(0.0, 0.0, 2.0, 2.0)
    // IllegalArgumentException thrown here since it's thrown on driver, not inside a task
    intercept[IllegalArgumentException](Statistics.chiSqTest(Matrices.dense(2, 2, colZero)))
  }
  //卡方卡方皮尔森 RDD标记点
  test("chi squared pearson RDD[LabeledPoint]") {
    // labels: 1.0 (2 / 6), 0.0 (4 / 6)
    // feature1: 0.5 (1 / 6), 1.5 (2 / 6), 3.5 (3 / 6)
    // feature2: 10.0 (1 / 6), 20.0 (1 / 6), 30.0 (2 / 6), 40.0 (2 / 6)
    val data = Seq(
      LabeledPoint(0.0, Vectors.dense(0.5, 10.0)),
      LabeledPoint(0.0, Vectors.dense(1.5, 20.0)),
      LabeledPoint(1.0, Vectors.dense(1.5, 30.0)),
      LabeledPoint(0.0, Vectors.dense(3.5, 30.0)),
      LabeledPoint(0.0, Vectors.dense(3.5, 40.0)),
      LabeledPoint(1.0, Vectors.dense(3.5, 40.0)))
    for (numParts <- List(2, 4, 6, 8)) {
      //提供了进行Pearson卡方检验的方法
      val chi = Statistics.chiSqTest(sc.parallelize(data, numParts))
      val feature1 = chi(0)
      //协方差
      assert(feature1.statistic === 0.75)
      assert(feature1.degreesOfFreedom === 2)
      assert(feature1.pValue ~== 0.6873 relTol 1e-4)
      assert(feature1.method === ChiSqTest.PEARSON.name)
      assert(feature1.nullHypothesis === ChiSqTest.NullHypothesis.independence.toString)
      val feature2 = chi(1)
      assert(feature2.statistic === 1.5)
      assert(feature2.degreesOfFreedom === 3)
      assert(feature2.pValue ~== 0.6823 relTol 1e-4)
      assert(feature2.method === ChiSqTest.PEARSON.name)
      assert(feature2.nullHypothesis === ChiSqTest.NullHypothesis.independence.toString)
    }

    // Test that the right number of results is returned
    //返回测试结果的正确数量
    val numCols = 1001
    val sparseData = Array(
      new LabeledPoint(0.0, Vectors.sparse(numCols, Seq((100, 2.0)))),
      new LabeledPoint(0.1, Vectors.sparse(numCols, Seq((200, 1.0)))))
    //提供了进行Pearson卡方检验的方法
    val chi = Statistics.chiSqTest(sc.parallelize(sparseData))
    assert(chi.size === numCols)
    assert(chi(1000) != null) // SPARK-3087

    // Detect continous features or labels
    //检测连续特征或标记
    val random = new Random(11L)
    val continuousLabel =
      Seq.fill(100000)(LabeledPoint(random.nextDouble(), Vectors.dense(random.nextInt(2))))
    intercept[SparkException] {
      //提供了进行Pearson卡方检验的方法
      Statistics.chiSqTest(sc.parallelize(continuousLabel, 2))
    }
    val continuousFeature =
      Seq.fill(100000)(LabeledPoint(random.nextInt(2), Vectors.dense(random.nextDouble())))
    intercept[SparkException] {
      //提供了进行Pearson卡方检验的方法
      Statistics.chiSqTest(sc.parallelize(continuousFeature, 2))
    }
  }

  test("1 sample Kolmogorov-Smirnov test: apache commons math3 implementation equivalence") {
    // Create theoretical distributions 创建理论分布
    val stdNormalDist = new NormalDistribution(0, 1)
    val expDist = new ExponentialDistribution(0.6)
    val unifDist = new UniformRealDistribution()

    // set seeds
    val seed = 10L
    stdNormalDist.reseedRandomGenerator(seed)
    expDist.reseedRandomGenerator(seed)
    unifDist.reseedRandomGenerator(seed)

    // Sample data from the distributions and parallelize it
    //从样本数据的分布和并行
    val n = 100000
    val sampledNorm = sc.parallelize(stdNormalDist.sample(n), 10)
    val sampledExp = sc.parallelize(expDist.sample(n), 10)
    val sampledUnif = sc.parallelize(unifDist.sample(n), 10)

    // Use a apache math commons local KS test to verify calculations
    val ksTest = new KolmogorovSmirnovTest()
    val pThreshold = 0.05

    // Comparing a standard normal sample to a standard normal distribution
    //将标准的正态样本与标准正态分布的比较
    val result1 = Statistics.kolmogorovSmirnovTest(sampledNorm, "norm", 0, 1)
    val referenceStat1 = ksTest.kolmogorovSmirnovStatistic(stdNormalDist, sampledNorm.collect())
    val referencePVal1 = 1 - ksTest.cdf(referenceStat1, n)
    // Verify vs apache math commons ks test
    //验证与Apache Commons KS检验数学
    assert(result1.statistic ~== referenceStat1 relTol 1e-4)
    assert(result1.pValue ~== referencePVal1 relTol 1e-4)
    // Cannot reject null hypothesis
    assert(result1.pValue > pThreshold)

    // Comparing an exponential sample to a standard normal distribution
    //指数样本与标准正态分布的比较
    val result2 = Statistics.kolmogorovSmirnovTest(sampledExp, "norm", 0, 1)
    val referenceStat2 = ksTest.kolmogorovSmirnovStatistic(stdNormalDist, sampledExp.collect())
    val referencePVal2 = 1 - ksTest.cdf(referenceStat2, n)
    // verify vs apache math commons ks test
    //验证与Apache Commons KS检验数学
    assert(result2.statistic ~== referenceStat2 relTol 1e-4)
    assert(result2.pValue ~== referencePVal2 relTol 1e-4)
    // reject null hypothesis
    //拒绝零假设
    assert(result2.pValue < pThreshold)

    // Testing the use of a user provided CDF function
    //测试一个用户提供的CDF函数的使用
    // Distribution is not serializable, so will have to create in the lambda
    //分布是不可序列化的，所以要在lambda创建
    val expCDF = (x: Double) => new ExponentialDistribution(0.2).cumulativeProbability(x)

    // Comparing an exponential sample with mean X to an exponential distribution with mean Y
    //指数样本与平均指数x的指数分布的比较
    // Where X != Y
    val result3 = Statistics.kolmogorovSmirnovTest(sampledExp, expCDF)
    val referenceStat3 = ksTest.kolmogorovSmirnovStatistic(new ExponentialDistribution(0.2),
      sampledExp.collect())
    val referencePVal3 = 1 - ksTest.cdf(referenceStat3, sampledNorm.count().toInt)
    // verify vs apache math commons ks test
    assert(result3.statistic ~== referenceStat3 relTol 1e-4)
    assert(result3.pValue ~== referencePVal3 relTol 1e-4)
    // reject null hypothesis
    assert(result3.pValue < pThreshold)
  }

  test("1 sample Kolmogorov-Smirnov test: R implementation equivalence") {
    /*
      Comparing results with R's implementation of Kolmogorov-Smirnov for 1 sample
      > sessionInfo()
      R version 3.2.0 (2015-04-16)
      Platform: x86_64-apple-darwin13.4.0 (64-bit)
      > set.seed(20)
      > v <- rnorm(20)
      > v
       [1]  1.16268529 -0.58592447  1.78546500 -1.33259371 -0.44656677  0.56960612
       [7] -2.88971761 -0.86901834 -0.46170268 -0.55554091 -0.02013537 -0.15038222
      [13] -0.62812676  1.32322085 -1.52135057 -0.43742787  0.97057758  0.02822264
      [19] -0.08578219  0.38921440
      > ks.test(v, pnorm, alternative = "two.sided")

               One-sample Kolmogorov-Smirnov test

      data:  v
      D = 0.18874, p-value = 0.4223
      alternative hypothesis: two-sided
    */

    val rKSStat = 0.18874
    val rKSPVal = 0.4223
    val rData = sc.parallelize(
      Array(
        1.1626852897838, -0.585924465893051, 1.78546500331661, -1.33259371048501,
        -0.446566766553219, 0.569606122374976, -2.88971761441412, -0.869018343326555,
        -0.461702683149641, -0.555540910137444, -0.0201353678515895, -0.150382224136063,
        -0.628126755843964, 1.32322085193283, -1.52135057001199, -0.437427868856691,
        0.970577579543399, 0.0282226444247749, -0.0857821886527593, 0.389214404984942))
    val rCompResult = Statistics.kolmogorovSmirnovTest(rData, "norm", 0, 1)
    assert(rCompResult.statistic ~== rKSStat relTol 1e-4)
    assert(rCompResult.pValue ~== rKSPVal relTol 1e-4)
  }
}
