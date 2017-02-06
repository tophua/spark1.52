package org.apache.spark.examples.mllib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.mllib.linalg.distributed.BlockMatrix
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.mllib.linalg.Vectors
/**
* 分块矩阵(BlockMatrix)是由RDD支撑的分布式矩阵,RDD中的元素为MatrixBlock,
* MatrixBlock是多个((Int, Int),Matrix)组成的元组,其中(Int,Int)是分块索引,Matriax是指定索引处的子矩阵
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
        Array(3.0, 80.0, 90.0, 100.0))).map(f =>{
          //take取前n个元素 drop舍弃前n个元素
          //1.0|||20.0,30.0,40.0
          println(f.take(1)(0)+"|||"+f.drop(1).mkString(","))
          IndexedRow(f.take(1)(0), Vectors.dense(f.drop(1)))
        })
    //索引行矩阵(IndexedRowMatrix)按行分布式存储,有行索引,其底层支撑结构是索引的行组成的RDD,所以每行可以通过索引(long)和局部向量表示
    val indexRowMatrix = new IndexedRowMatrix(rdd1)
    //将IndexedRowMatrix转换成BlockMatrix,指定每块的行列数
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
    //从转换后的内容可以看出,在indexRowMatrix.toBlockMatrix(2, 2)
    //操作时,指定行列数与实际矩阵内容不匹配时,会进行相应的零值填充
    //LocalMatrix局部矩阵使用整型行列索引和浮点(double)数值,存储在单机上
    blockMatrix.toLocalMatrix()

    //块矩阵相加
    blockMatrix.add(blockMatrix)

    //块矩阵相乘blockMatrix*blockMatrix^T（T表示转置）
    blockMatrix.multiply(blockMatrix.transpose)

    //转换成CoordinateMatrix
    //CoordinateMatrix常用于稀疏性比较高的计算中,MatrixEntry是一个 Tuple类型的元素,其中包含行、列和元素值
     blockMatrix.toCoordinateMatrix()

    //转换成IndexedRowMatrix
    blockMatrix.toIndexedRowMatrix()

    //验证分块矩阵的合法性
    blockMatrix.validate()
  }
}