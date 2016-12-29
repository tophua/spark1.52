package org.apache.spark.examples.mllib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.mllib.linalg.distributed.BlockMatrix

/**
 * 数据类型测试
 */
object DataTypes {
/**
 * 稀疏矩阵:在矩阵中，若数值为0的元素数目远远多于非0元素的数目时，则称该矩阵为稀疏矩阵
 * 密集矩阵:在矩阵中，若非0元素数目占大多数时，则称该矩阵为密集矩阵
 */
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkHdfsLR")
    val sc = new SparkContext(sparkConf)
    /**创建本地向量**/
    //本地向量（Local Vector）存储在单台机器上，索引采用0开始的整型表示，值采用Double类型的值表示
    // Create a dense vector (1.0, 0.0, 3.0).
    //密度矩阵，零值也存储
    val dv: Vector = Vectors.dense(1.0, 0.0, 3.0)
    // Create a sparse vector (1.0, 0.0, 3.0) by specifying its indices and values corresponding to nonzero entries.
   //创建稀疏矩阵，指定元素的个数、索引及非零值，数组方式
    val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
    // Create a sparse vector (1.0, 0.0, 3.0) by specifying its nonzero entries.
    // 创建稀疏矩阵，指定元素的个数、索引及非零值，采用序列方式
    val sv2: Vector = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))

    /**含标签点**/

    // Create a labeled point with a positive label and a dense feature vector.
    val pos = LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0))

    // Create a labeled point with a negative label and a sparse feature vector.
    val neg = LabeledPoint(0.0, Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0)))
    //稀疏数据,MLlib可以读取以LibSVM格式存储的训练实例,每行代表一个含类标签的稀疏特征向量
    //索引从1开始并且递增,加载被转换为从0开始   
    /**
 *  libSVM的数据格式
 *  <label> <index1>:<value1> <index2>:<value2> ...
 *  其中<label>是训练数据集的目标值,对于分类,它是标识某类的整数(支持多个类);对于回归,是任意实数
 *  <index>是以1开始的整数,可以是不连续
 *  <value>为实数,也就是我们常说的自变量
 */
    val examples: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
    /**本地密集矩阵***/
    // Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
    val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
    /**本地稀疏矩阵***/
    /**下列矩阵
    1.0 0.0 4.0

    0.0 3.0 5.0

    2.0 0.0 6.0
		如果采用稀疏矩阵存储的话，其存储信息包括按列的形式：
		实际存储值： [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]`,
		矩阵元素对应的行索引：rowIndices=[0, 2, 1, 0, 1, 2]`
		列起始位置索引： `colPointers=[0, 2, 3, 6]**/
    // Create a sparse matrix ((9.0, 0.0), (0.0, 8.0), (0.0, 6.0))
    val sm: Matrix = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 2, 1), Array(9, 6, 8))
    /**分布式矩阵**/
    val rows: RDD[Vector] = null // an RDD of local vectors
    // Create a RowMatrix from an RDD[Vector].
    //分布式矩阵行
    /**行分布式矩阵**/
    val mat: RowMatrix = new RowMatrix(rows)

    // Get its size.
    val m = mat.numRows()
    val n = mat.numCols()

    // QR decomposition 
    val qrResult = mat.tallSkinnyQR(true)
    /**索引行分布式矩阵**/
    //包涵行索引数据集信息
    val rowsIndex: RDD[IndexedRow] = null // an RDD of indexed rows 索引行的RDD
    // Create an IndexedRowMatrix from an RDD[IndexedRow].
    val matIndex: IndexedRowMatrix = new IndexedRowMatrix(rowsIndex)

    // Get its size. 得到它的大小
    val mIndex = matIndex.numRows()
    val nIndex = matIndex.numCols()

    // Drop its row indices. 下降行索引
    val rowMat: RowMatrix = matIndex.toRowMatrix()
    /***三元组矩阵*/
    val entries: RDD[MatrixEntry] = null // an RDD of matrix entries 矩阵元素的RDD
    // Create a CoordinateMatrix from an RDD[MatrixEntry].
    //创建一个坐标矩阵为RDD
    val matCoordinate: CoordinateMatrix = new CoordinateMatrix(entries)

    // Get its size.
    //得到它的大小
    val mCoordinate = mat.numRows()
    val nCoordinate = mat.numCols()

    // Convert it to an IndexRowMatrix whose rows are sparse vectors.
    //将其转换为一个行是稀疏向量的索引行矩阵
    val indexedRowMatrix = matCoordinate.toIndexedRowMatrix()
    /**BlockMatrix块矩阵**/

    val coordMat: CoordinateMatrix = new CoordinateMatrix(entries)

    val matA: BlockMatrix = coordMat.toBlockMatrix().cache()

    // Validate whether the BlockMatrix is set up properly. Throws an Exception when it is not valid.
    //验证是否正确设置属性,当它无效时抛出一个异常
    // Nothing happens if it is valid.如果它是有效的,什么都不会发生
    matA.validate()

    // Calculate A^T A.
    val ata = matA.transpose.multiply(matA)

  }
}