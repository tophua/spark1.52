package org.apache.spark.examples.mllib
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.mllib.regression.RidgeRegressionWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
/**
 * Lasso(套索)算法是线性回归中一种收缩和选择方法,它可最小化通常的平方差之和
 * 主要特征:做任意它认为没有用的预测因子,它会把它们的相关系数设为0从而把它们从方程式中删除
 */
object RidgeRegressionWithSGDExample {
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
    val model = RidgeRegressionWithSGD.train(rdd, 100, 0.02, 2.0)
    /**
     * 岭回归不会把预测因子系数设为0,但它会让它们近似于0
     * [0.049805969577244584,0.029883581746346748,0.009961193915448916,0.019922387830897833,
     *  0.009961193915448916,0.029883581746346748,0.019922387830897833,0.019922387830897833,
     *  0.009961193915448916]
     */
    //检查有多预测因子的系数被设为0,
    model.weights
  }
}