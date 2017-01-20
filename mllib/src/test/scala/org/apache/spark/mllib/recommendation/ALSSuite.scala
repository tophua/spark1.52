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

package org.apache.spark.mllib.recommendation

import scala.collection.JavaConversions._
import scala.math.abs
import scala.util.Random

import org.jblas.DoubleMatrix

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.storage.StorageLevel
/**
 * 矩阵分解:将用户(user)对商品(item)的评分矩阵分解为两个矩阵：一个是用户对商品隐含特征的偏好矩阵，另一个是商品所包含的隐含特征的矩阵。
 * 在这个矩阵分解的过程中，评分缺失项得到了填充，也就是说我们可以基于这个填充的评分来给用户最商品推荐了
 */
object ALSSuite {
/**
 * Llib中对每个解决最小二乘问题的正则化参数lambda做了扩展：
 * 一个是在更新用户因素时用户产生的评分数量;另一个是在更新产品因素时产品被评分的数量
 * numBlocks 是用于并行化计算的分块个数 (设置为-1,为自动配置)
 * rank ALS中因子的个数,通常来说越大越好,但是对内存占用率有直接影响,通常rank在10到200之间
 * iterations 迭代次数，每次迭代都会减少ALS的重构误差。在几次迭代之后,ALS模型都会收敛得到一个不错的结果,所以大多情况下不需要太多的迭代（通常是10次）
 * lambda 模型的正则化参数,控制着避免过度拟合,值越大,越正则化
 * implicitPrefs 决定了是用显性反馈ALS的版本还是用适用隐性反馈数据集的版本
 * alpha 是一个针对于隐性反馈 ALS 版本的参数,这个参数决定了偏好行为强度的基准
 * 调整这些参数,不断优化结果,使均方差变小.比如：iterations越多,lambda较小,均方差会较小,推荐结果较优
 */
  def generateRatingsAsJavaList(
      users: Int,
      products: Int,
      features: Int,
      samplingRate: Double,
      implicitPrefs: Boolean,
      negativeWeights: Boolean): (java.util.List[Rating], DoubleMatrix, DoubleMatrix) = {
    val (sampledRatings, trueRatings, truePrefs) =
      generateRatings(users, products, features, samplingRate, implicitPrefs)
      //seqAsJavaList将seq转换List
    (seqAsJavaList(sampledRatings), trueRatings, truePrefs)
  }
/**
 * 产生等级
 */
  def generateRatings(
      users: Int,//用户数
      products: Int,//产品数
      features: Int,//特征数
      samplingRate: Double,//采样等级
      implicitPrefs: Boolean = false,//特征列名
      negativeWeights: Boolean = false,//负数权重
      //负特征
      negativeFactors: Boolean = true): (Seq[Rating], DoubleMatrix, DoubleMatrix) = {
    val rand = new Random(42)
    // Create a random matrix with uniform values from -1 to 1
    //创建一个具有均匀值的随机矩阵从-1到1
    def randomMatrix(m: Int, n: Int) = {
      if (negativeFactors) {//负面因素
        new DoubleMatrix(m, n, Array.fill(m * n)(rand.nextDouble() * 2 - 1): _*)
      } else {
        new DoubleMatrix(m, n, Array.fill(m * n)(rand.nextDouble()): _*)
      }
    }
    /**
     * [0.455127; 0.366447; -0.382561; -0.445843; 0.331098; 0.806745; -0.262434; 
     *  -0.448504; -0.072693; 0.565804; 0.838656; -0.127018; 0.499812; -0.226866; 
     *  -0.697937; 0.667732; -0.079387; -0.438856; -0.608072; -0.641453;-0.026819]
     */
    val userMatrix = randomMatrix(users, features)//用户矩阵
    /**
     * [-0.158057, 0.265740, 0.399717, -0.369343, 0.143241, -0.257975, 0.743629, 
     *  0.611462, 0.232427, -0.251281, 0.394497, 0.817229, -0.607706, 0.618250, 
     *  -0.945372, -0.927031, -0.032312, 0.310107, -0.208467, 0.880345, -0.230778]
     */
    val productMatrix = randomMatrix(features, products)//产品矩阵
    /**
     * trueRatings
     * [-0.071936, 0.120945, 0.181922, -0.168098, 0.065193, -0.117412, 0.338446, 0.278293, 0.105784, 
     * -0.114365, 0.179547, 0.371943, -0.276584, 0.281382, 0.116452, -0.033316, -0.176972, 0.036328, 
     *  0.277983, -0.327883, -0.370522, -0.430523, 0.043997, -0.198255, 0.425540, -0.246466, 0.5...] 
     *  truePrefs:null
     */
    val (trueRatings, truePrefs) = implicitPrefs match {//隐式参数
      case true =>
        // Generate raw values from [0,9], or if negativeWeights, from [-2,7]
        //从[0,9]生成原始值,或者如果负权重,从[ -2,7 ]
        val raw = new DoubleMatrix(users, products,
          Array.fill(users * products)(
              //negativeWeights是否负权重
            (if (negativeWeights) -2 else 0) + rand.nextInt(10).toDouble): _*)
        val prefs =
          new DoubleMatrix(users, products, raw.data.map(v => if (v > 0) 1.0 else 0.0): _*)
        (raw, prefs)
        // mmul矩阵相乘,transpose返回转置矩阵复制
      case false => (userMatrix.mmul(productMatrix), null)
    }
    /**
     * 采样等级
     * Vector(Rating(0,0,-0.07193587706304089),Rating(0,1,0.12094535474220613),Rating(0,3,-0.1680981500089981), 
     * Rating(0,4,0.06519272116591945), Rating(0,5,-0.11741167215430767), Rating(0,6,0.3384459909907707),
     * Rating(0,15,-0.0333159863067016), Rating(0,16,-0.1769724912456509), Rating(0,17,0.03632775091236762))
     */
    val sampledRatings = {
      for (u <- 0 until users; p <- 0 until products if rand.nextDouble() < samplingRate) yield {
        /**
         * 46||94|||-0.5898041483232676
         * 46||99|||-0.042049378291736744
         * 47||0|||0.10138589042303103
         * 47||3|||0.2369162831251936
         * 47||4|||-0.09188213662446618
         */
        println(u+"||"+p+"|||"+trueRatings.get(u, p))
        Rating(u, p, trueRatings.get(u, p))
      }
    }

    (sampledRatings, trueRatings, truePrefs)
  }
}


class ALSSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("rank-1 matrices") {//矩阵潜在特征为1
    //用户数50,产品数100,特征数1,迭代次数15,采样率0.7 匹配阈值 0.3
    testALS(50, 100, 1, 15, 0.7, 0.3)
  }

  test("rank-1 matrices bulk") {//矩阵分解特征数,体积
    //用户数50,产品数100,特征数1,迭代次数15,采样率0.7 匹配阈值 0.3,bulk==true
    testALS(50, 100, 1, 15, 0.7, 0.3, false, true)
  }

  test("rank-2 matrices") {//矩阵的秩
    testALS(100, 200, 2, 15, 0.7, 0.3)
  }

  test("rank-2 matrices bulk") {//秩矩阵体积
    testALS(100, 200, 2, 15, 0.7, 0.3, false, true)
  }

  test("rank-1 matrices implicit") {//秩1隐式矩阵
    testALS(80, 160, 1, 15, 0.7, 0.4, true)
  }

  test("rank-1 matrices implicit bulk") {//秩1隐式大块
    testALS(80, 160, 1, 15, 0.7, 0.4, true, true)
  }

  test("rank-2 matrices implicit") {//秩2隐式矩阵
    testALS(100, 200, 2, 15, 0.7, 0.4, true)
  }

  test("rank-2 matrices implicit bulk") {//秩2隐式矩阵大块
    testALS(100, 200, 2, 15, 0.7, 0.4, true, true)
  }

  test("rank-2 matrices implicit negative") {//秩2隐式矩阵负数
    testALS(100, 200, 2, 15, 0.7, 0.4, true, false, true)
  }

  test("rank-2 matrices with different user and product blocks") {
  //numUserBlocks设置用户数据块的个数和并行度,numProductBlocks设置物品数据块个数和并行度
    testALS(100, 200, 2, 15, 0.7, 0.4, numUserBlocks = 4, numProductBlocks = 2)
  }

  test("pseudorandomness") {//伪随机性
    val ratings = sc.parallelize(ALSSuite.generateRatings(10, 20, 5, 0.5, false, false)._1, 2)
    val model11 = ALS.train(ratings, 5, 1, 1.0, 2, 1)//训练
    val model12 = ALS.train(ratings, 5, 1, 1.0, 2, 1)//训练
    //userFeatures用户特征
    val u11 = model11.userFeatures.values.flatMap(_.toList).collect().toList
    println("u11:"+u11.mkString(","))
    //userFeatures用户特征
    val u12 = model12.userFeatures.values.flatMap(_.toList).collect().toList
    println("u12:"+u12.mkString(","))
    val model2 = ALS.train(ratings, 5, 1, 1.0, 2, 2)
    //userFeatures用户特征
    val u2 = model2.userFeatures.values.flatMap(_.toList).collect().toList
    assert(u11 == u12)
    assert(u11 != u2)
  }

  test("Storage Level for RDDs in model") {//存储级别RDD模型
    val ratings = sc.parallelize(ALSSuite.generateRatings(10, 20, 5, 0.5, false, false)._1, 2)
    var storageLevel = StorageLevel.MEMORY_ONLY
    var model = new ALS()
      .setRank(5)
      .setIterations(1)
      .setLambda(1.0)
      .setBlocks(2)
      .setSeed(1)
      .setFinalRDDStorageLevel(storageLevel)
      .run(ratings)
    assert(model.productFeatures.getStorageLevel == storageLevel);
    assert(model.userFeatures.getStorageLevel == storageLevel);
    storageLevel = StorageLevel.DISK_ONLY
    model = new ALS()
      .setRank(5)//模型中潜在因素的数量
      .setIterations(1)//迭代次数
      .setLambda(1.0)//
      .setBlocks(2)
      .setSeed(1)
      .setFinalRDDStorageLevel(storageLevel)
      .run(ratings)
    assert(model.productFeatures.getStorageLevel == storageLevel);
    assert(model.userFeatures.getStorageLevel == storageLevel);
  }

  test("negative ids") {//负ID
    val data = ALSSuite.generateRatings(50, 50, 2, 0.7, false, false)
    val ratings = sc.parallelize(data._1.map {
      case Rating(u, p, r) =>Rating(u - 25, p - 25, r)
    })
    val correct = data._2
    //训练
    val model = ALS.train(ratings, 5, 15)

    val pairs = Array.tabulate(50, 50)((u, p) => (u - 25, p - 25)).flatten
    val ans = model.predict(sc.parallelize(pairs)).collect()
    ans.foreach { r =>
      val u = r.user + 25
      val p = r.product + 25
      val v = r.rating
      val error = v - correct.get(u, p)
       //math.abs返回数的绝对值
      assert(math.abs(error) < 0.4)
    }
  }

  test("NNALS, rank 2") {
    testALS(100, 200, 2, 15, 0.7, 0.4, false, false, false, -1, -1, false)
  }

  /**
   * Test if we can correctly factorize R = U * P where U and P are of known rank.
   * 如果我们能正确地分解因子R = U * P 这里U和P已知潜在特征数
   * @param users number of users 用户标识
   * @param products number of products 产品标识
   * @param features number of features (rank of problem 排名的问题)特征数
   * @param iterations number of iterations to run 运行的迭代次数
   * @param samplingRate what fraction of the user-product pairs are known
   * 				哪一部分的用户产品的已知
   * @param matchThreshold max difference allowed to consider a predicted rating correct
   * 				最大允许差异考虑预测评级正确
   * @param implicitPrefs flag to test implicit feedback 标志测试隐式反馈
   * @param bulkPredict flag to test bulk predicition 批量测试产品标志
   * @param negativeWeights whether the generated data can contain negative values
   * 				生成的数据是否可以包含负值
   * @param numUserBlocks number of user blocks to partition users into
   * 				设置用户数据块的个数和并行度
   * @param numProductBlocks number of product blocks to partition products into
   * 				设置物品数据块个数和并行度
   * @param negativeFactors whether the generated user/product factors can have negative entries
   * 				是否生成的用户/产品因素可以有负面的条目
   */
  // scalastyle:off
  def testALS(
      users: Int,//用户数
      products: Int,//产品数
      features: Int,//特征数
      iterations: Int,//迭代次数
      samplingRate: Double,//抽样率
      matchThreshold: Double,//匹配阈值
      implicitPrefs: Boolean = false,//制定是否使用显示反馈ALS变体(或者说是对隐式反馈数据的一种适应)
      bulkPredict: Boolean = false,//大部分预测
      negativeWeights: Boolean = false,//负权重
      numUserBlocks: Int = -1,//设置用户数据块的个数和并行度(默认值为-1,表示自动配置)
      numProductBlocks: Int = -1,//设置物品数据块个数和并行度
      negativeFactors: Boolean = true) {//负因子
    // scalastyle:on
     /**
     * trueRatings 真正的评级
     * [-0.071936, 0.120945, 0.181922, -0.168098, 0.065193, -0.117412, 0.338446, 0.278293, 0.105784, 
     * -0.114365, 0.179547, 0.371943, -0.276584, 0.281382, 0.116452, -0.033316, -0.176972, 0.036328, 
     *  0.277983, -0.327883, -0.370522, -0.430523, 0.043997, -0.198255, 0.425540, -0.246466, 0.5...] 
     * sampledRatings 采样率   
     * Vector(Rating(0,0,-0.07193587706304089),Rating(0,1,0.12094535474220613),Rating(0,3,-0.1680981500089981), 
     * Rating(0,4,0.06519272116591945), Rating(0,5,-0.11741167215430767), Rating(0,6,0.3384459909907707),
     * Rating(0,15,-0.0333159863067016), Rating(0,16,-0.1769724912456509), Rating(0,17,0.03632775091236762))
     * truePrefs:null
     */
    val (sampledRatings, trueRatings, truePrefs) = ALSSuite.generateRatings(users, products,
      features, samplingRate, implicitPrefs, negativeWeights, negativeFactors)

    val model = new ALS()
      .setUserBlocks(numUserBlocks)//设置用户数据块的个数和并行度
      .setProductBlocks(numProductBlocks)//设置物品数据块个数和并行度
      .setRank(features)//模型中潜在因素的数量
      .setIterations(iterations)//迭代次数
      .setAlpha(1.0)//应用于隐式数据的ALS变体,它控制的是观察到偏好的基本置信度
      .setImplicitPrefs(implicitPrefs)//决定了是用显性,还是隐性反馈数据集的版本
      .setLambda(0.01)//ALS的正则化参数
      .setSeed(0L)//
      .setNonnegative(!negativeFactors)//是否需要非负约束
      .run(sc.parallelize(sampledRatings))
    /**
     *[0.436768; 0.352116; -0.368858; -0.430065; 0.317578; 0.774760; -0.253319; -0.432301; -0.069874; 0.545266; 
     * 0.805510; -0.122209; 0.482757; -0.218839; -0.621789; 0.181787; -0.559713; 0.628474; -0.631657; 0.168301; 
     * 0.483690; 0.136887; 0.154252; 0.485914; -0.900840; -0.274579; 0.613773; -0.158895; 0.912472; 0.411233;]
     */
    val predictedU = new DoubleMatrix(users, features)
    /**
     * u:0|||0|||0.436768114566803
		 * u:2|||0|||-0.3688576817512512
		 * u:4|||0|||0.3175782263278961
		 * u:6|||0|||-0.2533193826675415
		 * scala中还有一个和上面的to关键字有类似作用的关键字until,它的不同之处在于不包括最后一个元素
     */
    for ((u, vec) <- model.userFeatures.collect(); i <- 0 until features) {
      //println("u:"+u+"|||"+i+"|||"+vec(i))
      predictedU.put(u, i, vec(i))
    }
    /**
     [-0.159104; 0.266854; 0.402118; -0.371319; 0.144135; -0.260481; 0.750064; 0.608005; 0.232232; -0.252072; 
     0.397453; 0.823892; -0.614564; 0.621769; 0.258227; -0.073655; -0.384947; 0.079571; 0.271885; -0.751692; 
     -0.005389; -0.950375; -0.928237; -0.032716; 0.312536; -0.209477; 0.888935; -0.231828; 0.293711; 0.542644;
     ]**/
    val predictedP = new DoubleMatrix(products, features)
    /**
   	 * p:0|||0|||-0.15910351276397705
  	 * p:2|||0|||0.4021179974079132
  	 * p:4|||0|||0.14413467049598694
  	 * p:6|||0|||0.7500635981559753
  	 * p:8|||0|||0.23223192989826202
  	 * p:10|||0|||0.39745309948921204
  	 * scala中还有一个和上面的to关键字有类似作用的关键字until,它的不同之处在于不包括最后一个元素
     */
    for ((p, vec) <- model.productFeatures.collect(); i <- 0 until features) {
      //println("p:"+p+"|||"+i+"|||"+vec(i))
      predictedP.put(p, i, vec(i))
    }
    /**
     * [-0.069491, 0.116553, 0.175632, -0.162180, 0.062953, -0.113770, 0.327604, 0.265557, 0.101432, -0.110097,
     *  0.173595, 0.359850, -0.268422, 0.271569, 0.112785, -0.032170, -0.168132, 0.034754, 0.118751, -0.328315,
     *  -0.002354, -0.415093, -0.405424, -0.014289, 0.136506, -0.091493, 0.388259, -0.101255, 0.128284,0.237009]
     */
    val predictedRatings = bulkPredict match {
      /**
       * mmul矩阵相乘,transpose返回转置矩阵复制
       */
      case false => predictedU.mmul(predictedP.transpose)
      case true =>
        //创建一个矩阵,预测等级
        val allRatings = new DoubleMatrix(users, products)
        /**        
         * usersProducts:IndexedSeq[(Int, Int)]
         * 0|||||||154
         * 0|||||||155
         * 0|||||||156      
         * 1|||||||0
         * 1|||||||1
         * 1|||||||2
         * 1|||||||3
         * scala中还有一个和上面的to关键字有类似作用的关键字until,它的不同之处在于不包括最后一个元素
         * for循环中的 yield 会把当前的元素记下来,保存在集合中,循环结束后将返回该集合.
         * Scala中 for循环是有返回值的,如果被循环的是 Map,返回的就是  Map,被循环的是 List,返回的就是 List
         */
        val usersProducts = for (u <- 0 until users; p <- 0 until products)  yield{
          //println(u+"|||||||"+p)
          (u, p)
        }            
        val userProductsRDD = sc.parallelize(usersProducts)
        //预测
        model.predict(userProductsRDD).collect().foreach { elem =>
        //矩阵的预测的评级
          allRatings.put(elem.user, elem.product, elem.rating)
        }
        allRatings
    }

    if (!implicitPrefs) {//是否使用隐式特征
      for (u <- 0 until users; p <- 0 until products) {
        //获得矩阵行和列数
        val prediction = predictedRatings.get(u, p)
        val correct = trueRatings.get(u, p)
        //abs返回数的绝对值
        if (math.abs(prediction - correct) > matchThreshold) {
          fail(("Model failed to predict (%d, %d): %f vs %f\ncorr: %s\npred: %s\nU: %s\n P: %s")
            .format(u, p, correct, prediction, trueRatings, predictedRatings, predictedU,
              predictedP))
        }
      }
    } else {
      // For implicit prefs we use the confidence-weighted RMSE to test (ref Mahout's tests)
      //隐式特征首选项参数我们用欺诈加权均方根误差测试
      var sqErr = 0.0
      var denom = 0.0
      for (u <- 0 until users; p <- 0 until products) {
        //获得矩阵行和列数
        val prediction = predictedRatings.get(u, p)
        val truePref = truePrefs.get(u, p)
        //abs返回数的绝对值
        val confidence = 1 + 1.0 * abs(trueRatings.get(u, p))
        //
        val err = confidence * (truePref - prediction) * (truePref - prediction)
        sqErr += err
        denom += confidence
      }
      //math.sqrt返回数字的平方根
      //rmse 均方根误差亦称标准误差,
     //均方根误差常用下式表示：√[∑di^2/n]=Re,式中：n为测量次数;di为一组测量值与真值的偏差
     //均方根值(RMS)、均方根误差(RMSE)
      val rmse = math.sqrt(sqErr / denom)
       //在二进制分类中设置阈值,范围为[0，1],如果类标签1的估计概率>Threshold,则预测1,否则0
      if (rmse > matchThreshold) {
        fail("Model failed to predict RMSE: %f\ncorr: %s\npred: %s\nU: %s\n P: %s".format(
          rmse, truePrefs, predictedRatings, predictedU, predictedP))
      }
    }
  }
}

