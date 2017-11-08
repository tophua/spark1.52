package org.apache.spark.examples.mllib

import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Matrix
/**
 * 基本统计功能
 */
object BasicStatistics {

  def main(args: Array[String]) {
    val observations: RDD[Vector] = null // an RDD of Vectors 一个向量的RDD
    // Compute column summary statistics. 计算列汇总统计
    //返回一个MultivariateStatisticalSummary实例里面包括面向列的最大值
    //最小值,均值,方差,非零值个数及总数
    val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)
    //一个包含每个列的平均值的稠密向量
    println(summary.mean) // a dense vector containing the mean value for each column
    println(summary.variance) // column-wise variance 列的方差
    println(summary.numNonzeros) // number of nonzeros in each column 在每一列的矩阵的非零元素数

    /***提供列间相关性**/
    val sc: SparkContext = null
    val seriesX: RDD[Double] = null // a series 一系列
    //必须与seriesX具有相同数量的分区和基数
    val seriesY: RDD[Double] = null // must have the same number of partitions and cardinality as seriesX

    // compute the correlation using Pearson's method. Enter "spearman" for Spearman's method. If a 
    // method is not specified, Pearson's method will be used by default. 
    //pearson皮尔森相关性
    val correlation: Double = Statistics.corr(seriesX, seriesY, "pearson")
    println("pearson:"+correlation)
    //请注意,每个向量是一个行,而不是一个列
    val data: RDD[Vector] = null // note that each Vector is a row and not a column 
     //spearman 斯皮尔曼相关性
    // calculate the correlation matrix using Pearson's method. Use "spearman" for Spearman's method.
    //用皮尔森法计算相关矩阵,用“斯皮尔曼”的斯皮尔曼方法
    // If a method is not specified, Pearson's method will be used by default. 
    //如果没有指定方法,皮尔森的方法将被默认使用
    val correlMatrix: Matrix = Statistics.corr(data, "pearson")
    println("correlMatrix:"+correlMatrix.toString())


  }
}
