package org.apache.spark.examples.mllib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics

object StatisticsDemo {
  def main(args: Array[String]) {
    //val sparkConf = new SparkConf().setMast("local[2]").setAppName("SparkHdfsLR")

    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(
      Array(
        Array(1.0, 2.0, 3.0),
        Array(2.0, 3.0, 4.0))).map(f =>Vectors.dense(f))
    //比如1.2.3.4.5 这五个数的平均数是3
    val mss  = Statistics.colStats(rdd1)
    //方差是各个数据与平均数之差的平方相加再除以个数
    //方差越小越稳定,表示数据间差别小
    println("均值:" + mss.mean);
    println("样本方差:" + mss.variance); //样本方差是各个数据与平均数之差的平方相加再除以(个数-1)
    println("非零统计量个数:" + mss.numNonzeros);
    println("总数:" + mss.count);
    println("最大值:" + mss.max);
    println("最小值:" + mss.min);
    //其它normL2等统计信息
    /**
     * 假设有两块土地，通过下列数据来检验其开红花的比率是否相同：
     * 	土地一， 开红花:1000，开兰花:1856
     * 	土地二， 开红花:400.，开兰花:560
     */
    val land1 = Vectors.dense(1000.0, 1856.0)
    val land2 = Vectors.dense(400, 560)
    val c1 = Statistics.chiSqTest(land1, land2)

  }
}