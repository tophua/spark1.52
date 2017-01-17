package org.apache.spark.examples.mllib

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
/**
 * 线性回归
 */
object LinearRegressionDemo {
  def main(args: Array[String]): Unit = {

    // 屏蔽不必要的日志显示终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    // 设置运行环境
    val conf = new SparkConf().setAppName("LinearRegressionDemo").setMaster("local[4]")
    val sc = new SparkContext(conf)
    // 加载数据
    val data = sc.textFile("../data/mllib/ridge-data/lpsa.data")

    val parsedData = data.map { line =>

      val parts = line.split(',')
	//LabeledPoint标记点是局部向量,向量可以是密集型或者稀疏型,每个向量会关联了一个标签(label)
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))

    }

    // Building the model
    //构建数据模型

    val numIterations = 100

    val model = LinearRegressionWithSGD.train(parsedData, numIterations)

    // Evaluate model on training examples and compute training error
    //使用训练样本计算模型并且计算训练误差
    val valuesAndPreds = parsedData.map { point =>

      val prediction = model.predict(point.features)
      //println("point.label:"+point.label+"\t prediction:"+prediction)
      (point.label, prediction)
    }
    //均方误差
    val MSE = valuesAndPreds.map { case (x, y) =>
           var w=math.pow((x - y), 2)
           println("x:"+x+"\t y:"+y+"\t x-y:"+(x - y)+"\t pow:"+math.pow((x - y), 2))
           w      
    }.reduce(_ + _) / valuesAndPreds.count //
    //均方误差,第二种方式均方差来评估预测值
    val MSE2 = valuesAndPreds.map { case (v, p) => math.pow((v - p), 2)}.mean() //求平均值
    println("training Mean Squared Error = " + MSE + "\t MSE2:" + MSE2)

    sc.stop()

  }
}