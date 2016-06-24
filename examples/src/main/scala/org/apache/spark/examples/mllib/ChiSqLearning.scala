package org.apache.spark.examples.mllib
import org.apache.spark.mllib.linalg.{ Matrix, Matrices, Vectors }
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.{
  SparkConf,
  SparkContext

}
/**
 * 卡方检验就是统计样本的实际观测值与理论推断值之间的偏离程度，实际观测值与理论推断值之间的偏离程度就决定卡方值的大小，
 * 卡方值越大，越不符合；卡方值越小，偏差越小，越趋于符合，
 * 若两个值完全相等时，卡方值就为0，表明理论值完全符合
 * 自由度v=（行数-1）（列数-1）=1
 */
object ChiSqLearning {
  def main(args: Array[String]) {
    val vd = Vectors.dense(1, 2, 3, 4, 5)
    val vdResult = Statistics.chiSqTest(vd)
    println(vd)
    println(vdResult)
    println("-------------------------------")
    val mtx = Matrices.dense(3, 2, Array(1, 3, 5, 2, 4, 6))
    val mtxResult = Statistics.chiSqTest(mtx)
    println(mtx)
    println(mtxResult)
    
    //print :方法、自由度、方法的统计量、p值,推论犯错的概率p
    println("-------------------------------")
    val mtx2 = Matrices.dense(2, 2, Array(19.0, 34, 24, 10.0))
    printChiSqTest(mtx2)
    printChiSqTest(Matrices.dense(2, 2, Array(26.0, 36, 7, 2.0)))
    //    val mtxResult2 = Statistics.chiSqTest(mtx2)
    //    println(mtx2)
    //    println(mtxResult2)
  }

  def printChiSqTest(matrix: Matrix): Unit = {
    println("-------------------------------")
    val mtxResult2 = Statistics.chiSqTest(matrix)
    println(matrix)
    println(mtxResult2)
  }

}