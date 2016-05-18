package org.apache.spark.examples.mllib

import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.mllib.linalg.Vectors

object IndexRowMatrixDemo {
  def main(args: Array[String]) {
    //val sparkConf = new SparkConf().setMast("local[2]").setAppName("SparkHdfsLR")

    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    //定义一个隐式转换函数
    implicit def double2long(x: Double) = x.toLong
    //数据中的第一个元素为IndexedRow中的index，剩余的映射到vector
    //f.take(1)(0)获取到第一个元素并自动进行隐式转换，转换成Long类型
    val rdd1 = sc.parallelize(
      Array(
        Array(1.0, 2.0, 3.0, 4.0),
        Array(2.0, 3.0, 4.0, 5.0),
        Array(3.0, 4.0, 5.0, 6.0))).map(f => IndexedRow(f.take(1)(0), Vectors.dense(f.drop(1))))
    val indexRowMatrix = new IndexedRowMatrix(rdd1)
    //计算拉姆矩阵
    var gramianMatrix: Matrix = indexRowMatrix.computeGramianMatrix()
    //转换成行矩阵RowMatrix
    var rowMatrix: RowMatrix = indexRowMatrix.toRowMatrix()
    //其它方法例如computeSVD计算奇异值、multiply矩阵相乘等操作，方法使用与RowMaxtrix相同
  }
}