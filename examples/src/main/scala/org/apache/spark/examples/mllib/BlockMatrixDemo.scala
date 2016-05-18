package org.apache.spark.examples.mllib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.mllib.linalg.distributed.BlockMatrix
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.mllib.linalg.Vectors
/**
 * BlockMatrix的使用
 * 分块矩阵将一个矩阵分成若干块
 */
object BlockMatrixDemo {
  def main(args: Array[String]) {
    //val sparkConf = new SparkConf().setMast("local[2]").setAppName("SparkHdfsLR")

    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    implicit def double2long(x: Double) = x.toLong
    val rdd1 = sc.parallelize(
      Array(
        Array(1.0, 20.0, 30.0, 40.0),
        Array(2.0, 50.0, 60.0, 70.0),
        Array(3.0, 80.0, 90.0, 100.0))).map(f => IndexedRow(f.take(1)(0), Vectors.dense(f.drop(1))))
    val indexRowMatrix = new IndexedRowMatrix(rdd1)
    //将IndexedRowMatrix转换成BlockMatrix，指定每块的行列数
    val blockMatrix: BlockMatrix = indexRowMatrix.toBlockMatrix(2, 2)

    //执行后的打印内容：
    //Index:(0,0)MatrixContent:2 x 2 CSCMatrix
    //(1,0) 20.0
    //(1,1) 30.0
    //Index:(1,1)MatrixContent:2 x 1 CSCMatrix
    //(0,0) 70.0
    //(1,0) 100.0
    //Index:(1,0)MatrixContent:2 x 2 CSCMatrix
    //(0,0) 50.0
    //(1,0) 80.0
    //(0,1) 60.0
    //(1,1) 90.0
    //Index:(0,1)MatrixContent:2 x 1 CSCMatrix
    //(1,0) 40.0
    //从打印内容可以看出：各分块矩阵采用的是稀疏矩阵CSC格式存储
    blockMatrix.blocks.foreach(f => println("Index:" + f._1 + "MatrixContent:" + f._2))

    //转换成本地矩阵
    //0.0   0.0   0.0    
    //20.0  30.0  40.0   
    //50.0  60.0  70.0   
    //80.0  90.0  100.0 
    //从转换后的内容可以看出，在indexRowMatrix.toBlockMatrix(2, 2)
    //操作时，指定行列数与实际矩阵内容不匹配时，会进行相应的零值填充
    blockMatrix.toLocalMatrix()

    //块矩阵相加
    blockMatrix.add(blockMatrix)

    //块矩阵相乘blockMatrix*blockMatrix^T（T表示转置）
    blockMatrix.multiply(blockMatrix.transpose)

    //转换成CoordinateMatrix
    blockMatrix.toCoordinateMatrix()

    //转换成IndexedRowMatrix
    blockMatrix.toIndexedRowMatrix()

    //验证分块矩阵的合法性
    blockMatrix.validate()
  }
}