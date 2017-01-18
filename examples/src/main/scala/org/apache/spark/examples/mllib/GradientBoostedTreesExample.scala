package org.apache.spark.examples.mllib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.{BoostingStrategy, Algo}
import org.apache.spark.mllib.util.MLUtils
/**
 *Spark coolbook p139 
 * 梯度提升决策树:综合多个决策树,消除噪声,避免过拟合,GBT只适用于二分类和回归,不支持多分类
 * 梯度提升决策树算法一次训练一个树,其中每个新的树会基于之前训练过树的缺点来改进算法
 * 梯度提升决策树算法目的是给一个人预测他是否拥有良好的信用
 * 标记是良好的信用
 * 姓名	标记	慷慨	   责任感	爱心  条理性	  挥霍	易怒
        杰克		0		0		0			0			0				1			1
        杰西卡   1		1		1			1			1				0			0
        珍妮		0		0		0			1			0				1			1
        瑞克		1		1		1			0			1				0			0
        帕特		0		0		0			0			0				1			1
        杰布		1		1		1			1			0				0			0
        杰伊		1		0		1			1			1				0			0
        纳特		0		1		0			0			0				1			1
        罗恩		1		0		1			1			1				0			0
        麦特		0		1		0			0			0				1			1
      使用libsvm格式表示方式
        0 5:1 6:1
        1 1:1 2:1 3:1 4:1
        0 3:1 5:1 6:1
        1 1:1 2:1 4:1
        0 5:1 6:1
        1 1:1 2:1 3:1 4:1
        0 1:1 5:1 6:1
        1 2:1 3:1 4:1
        0 1:1 5:1 6:1
 **/
object GradientBoostedTreesExample {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkHdfsLR")
    val sc = new SparkContext(sparkConf)
 /**
 *  libSVM的数据格式
 *  <label> <index1>:<value1> <index2>:<value2> ...
 *  其中<label>是训练数据集的目标值,对于分类,它是标识某类的整数(支持多个类);对于回归,是任意实数
 *  <index>是以1开始的整数,可以是不连续
 *  <value>为实数,也就是我们常说的自变量
 */
    // 加载数据
    val data = MLUtils.loadLibSVMFile(sc, "../data/mllib/rf_libsvm_data.txt")
    // 将数据随机分配为两份，一份用于训练，一份用于测试
    val splits = data.randomSplit(Array(0.7, 0.3))
    //数据分成训练和测试数据集
    val (trainingData, testData) = (splits(0), splits(1))
    //创建一个分类的提升策略并设置迭代次数为3(随机森林也支持回归)
    val boostingStrategy =BoostingStrategy.defaultParams("Classification")
        boostingStrategy.numIterations = 3
    //梯度提升决策树:综合多个决策树,消除噪声,避免过拟合
    val model = GradientBoostedTrees.train(trainingData,boostingStrategy)
    //基于测试实例评估模型并计算测试错误
    val testErr = testData.map { point =>
            //预测
            val prediction = model.predict(point.features)
            if (point.label == prediction) 
                1.0 
            else 0.0}.mean()//平均数
    //检查模型
    println("Test Error = " + testErr)
    println("Learned Random Forest:n" + model.toDebugString)
  }
}