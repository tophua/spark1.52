package org.apache.spark.examples.mllib
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
/**
 *2006年对日本不同沙滩上蜘蛛的分布做一些研究
 * 关于蜘蛛的粒度大小和现存状态的数据
 * 0表示消失,1表示存在
 */
object LogisticRegressionWithLBFGSExample {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("CountVectorizerExample").setMaster("local[4]")
    val sc = new SparkContext(conf)
    /**
     * 以蜘蛛的存在或消失作为标记创建一个LabledPoint数组
     */
    val points = Array(
    //LabeledPoint标记点是局部向量,向量可以是密集型或者稀疏型,每个向量会关联了一个标签(label)
      LabeledPoint(0.0,Vectors.dense(0.245)),
      LabeledPoint(0.0,Vectors.dense(0.247)),
      LabeledPoint(1.0,Vectors.dense(0.285)),
      LabeledPoint(1.0,Vectors.dense(0.299)),
      LabeledPoint(1.0,Vectors.dense(0.327)),
      LabeledPoint(1.0,Vectors.dense(0.347)),
      LabeledPoint(0.0,Vectors.dense(0.356)),
      LabeledPoint(1.0,Vectors.dense(0.36)),
      LabeledPoint(0.0,Vectors.dense(0.363)),
      LabeledPoint(1.0,Vectors.dense(0.364)),
      LabeledPoint(0.0,Vectors.dense(0.398)),
      LabeledPoint(1.0,Vectors.dense(0.4)),
      LabeledPoint(0.0,Vectors.dense(0.409)),
      LabeledPoint(1.0,Vectors.dense(0.421)),
      LabeledPoint(0.0,Vectors.dense(0.432)),
      LabeledPoint(1.0,Vectors.dense(0.473)),
      LabeledPoint(1.0,Vectors.dense(0.509)),
      LabeledPoint(1.0,Vectors.dense(0.529)),
      LabeledPoint(0.0,Vectors.dense(0.561)),
      LabeledPoint(0.0,Vectors.dense(0.569)),
      LabeledPoint(1.0,Vectors.dense(0.594)),
      LabeledPoint(1.0,Vectors.dense(0.638)),
      LabeledPoint(1.0,Vectors.dense(0.656)),
      LabeledPoint(1.0,Vectors.dense(0.816)),
      LabeledPoint(1.0,Vectors.dense(0.853)),
      LabeledPoint(1.0,Vectors.dense(0.938)),
      LabeledPoint(1.0,Vectors.dense(1.036)),
      LabeledPoint(1.0,Vectors.dense(1.045)))
    //创建之前数据的RDD
    val spiderRDD = sc.parallelize(points)
    //使用数据训练模型(当所有预测值为0的时候,拦截是有意义的)
    //逻辑回归,基于lbfgs优化损失函数,支持多分类,(BFGS是逆秩2拟牛顿法)
    val lr = new LogisticRegressionWithLBFGS().setIntercept(true)
    val model = lr.run(spiderRDD)
    //预测0.938尺度的蜘蛛的现状
    val predict = model.predict(Vectors.dense(0.938))
 
  }
}
