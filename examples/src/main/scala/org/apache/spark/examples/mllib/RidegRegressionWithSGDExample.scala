package org.apache.spark.examples.mllib
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.mllib.regression.LassoWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
/**
 * Lasso(套索)算法是线性回归中一种收缩和选择方法,它可最小化通常的平方差之和
 * 主要特征:做任意它认为没有用的预测因子,它会把它们的相关系数设为0从而把它们从方程式中删除
 */
object LassoExample {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("CountVectorizerExample").setMaster("local[4]")
    val sc = new SparkContext(conf)
    /**
     * 创建以病态数据体作为标记的LabledPoint数组
     */
    val points = Array(
    //LabeledPoint标记点是局部向量,向量可以是密集型或者稀疏型,每个向量会关联了一个标签(label)
      LabeledPoint(1, Vectors.dense(5, 3, 1, 2, 1, 3, 2, 2, 1)),
      LabeledPoint(2, Vectors.dense(9, 8, 8, 9, 7, 9, 8, 7, 9)))
    //创建之前数据的RDD
    val rdd = sc.parallelize(points)
    //使用数据迭代100次训练模型,这时步长和正则化参数已经手动设置好了
    val model = LassoWithSGD.train(rdd, 100, 0.02, 2.0)
    /**
     * 9个预测因子中有6个的系数被设成了0,这是LassoW主要特征:做任意它认为没有用的预测因子,
     * 它会把它们的相关系数设为0从而把它们从方程式中删除
     * [0.13455106581619633,0.0224732644670294,0.0,0.0,0.0,0.01360995990267153,0.0,0.0,0.0]
     */
    //检查有多预测因子的系数被设为0,
    model.weights
  }
}