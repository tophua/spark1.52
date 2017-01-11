package org.apache.spark.examples.mllib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.Matrices

object RowMatrixDedmo {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkHdfsLR")
    val sc = new SparkContext(sparkConf)
    // 创建RDD[Vector]
    val rdd1 = sc.parallelize(
      Array(
        Array(1.0, 2.0, 3.0, 4.0),
        Array(2.0, 3.0, 4.0, 5.0),
        Array(3.0, 4.0, 5.0, 6.0))).map(f => Vectors.dense(f))
    //创建分布式矩阵 RowMatrix
    val rowMatirx = new RowMatrix(rdd1)
    //计算列之间的相似度，返回的是CoordinateMatrix，采用
    //case class MatrixEntry(i: Long, j: Long, value: Double)存储值
    //columnSimilarities计算矩阵中每两列之间的余弦相似度
    //参数:使用近似算法的阈值,值越大则运算速度越快而误差越大,默认为0
    var coordinateMatrix: CoordinateMatrix = rowMatirx.columnSimilarities()
    //返回矩阵行数、列数
    println(coordinateMatrix.numCols())
    println(coordinateMatrix.numRows())
    //查看返回值，查看列与列之间的相似度
    //Array[org.apache.spark.mllib.linalg.distributed.MatrixEntry] 
    //= Array(MatrixEntry(2,3,0.9992204753914715), 
    //MatrixEntry(0,1,0.9925833339709303), 
    //MatrixEntry(1,2,0.9979288897338914), 
    //MatrixEntry(0,3,0.9746318461970762), 
    //MatrixEntry(1,3,0.9946115458726394), 
    //MatrixEntry(0,2,0.9827076298239907))
    println(coordinateMatrix.entries.collect())

    //转成后块矩阵，下一节中详细讲解
    coordinateMatrix.toBlockMatrix()
    //转换成索引行矩阵，下一节中详细讲解
    coordinateMatrix.toIndexedRowMatrix()
    //转换成RowMatrix
    coordinateMatrix.toRowMatrix()

    //计算列统计信息
    var mss: MultivariateStatisticalSummary = rowMatirx.computeColumnSummaryStatistics()
    //每列的均值, org.apache.spark.mllib.linalg.Vector = [2.0,3.0,4.0,5.0]
    mss.mean
    // 每列的最大值org.apache.spark.mllib.linalg.Vector = [3.0,4.0,5.0,6.0]
    mss.max
    // 每列的最小值 org.apache.spark.mllib.linalg.Vector = [1.0,2.0,3.0,4.0]
    mss.min
    //每列非零元素的个数org.apache.spark.mllib.linalg.Vector = [3.0,3.0,3.0,3.0]
    mss.numNonzeros
    //矩阵列的1-范数,||x||1 = sum（abs(xi)）；
    //org.apache.spark.mllib.linalg.Vector = [6.0,9.0,12.0,15.0]
    mss.normL1
    //矩阵列的2-范数,||x||2 = sqrt(sum(xi.^2))；
    // org.apache.spark.mllib.linalg.Vector = [3.7416573867739413,5.385164807134504,7.0710678118654755,8.774964387392123]
    mss.normL2
    //矩阵列的方差
    //org.apache.spark.mllib.linalg.Vector = [1.0,1.0,1.0,1.0]
    mss.variance
    //计算协方差
    //covariance: org.apache.spark.mllib.linalg.Matrix = 
    //1.0  1.0  1.0  1.0  
    //1.0  1.0  1.0  1.0  
    //1.0  1.0  1.0  1.0  
    //1.0  1.0  1.0  1.0  
    //computeCovariance 计算矩阵中行向量的协方差
    var covariance: Matrix = rowMatirx.computeCovariance()
    //计算拉姆矩阵rowMatirx^T*rowMatirx，T表示转置操作
    //gramianMatrix: org.apache.spark.mllib.linalg.Matrix = 
    //14.0  20.0  26.0  32.0  
    //20.0  29.0  38.0  47.0  
    //26.0  38.0  50.0  62.0  
    //32.0  47.0  62.0  77.0  
    var gramianMatrix: Matrix = rowMatirx.computeGramianMatrix()
    //对矩阵进行主成分分析，参数指定返回的列数，即主分成个数
    //PCA算法是一种经典的降维算法
    //principalComponents: org.apache.spark.mllib.linalg.Matrix = 
    //-0.5000000000000002  0.8660254037844388    
    //-0.5000000000000002  -0.28867513459481275  
    //-0.5000000000000002  -0.28867513459481287  
    //-0.5000000000000002  -0.28867513459481287  
    var principalComponents = rowMatirx.computePrincipalComponents(2)

    /**
     * 对矩阵进行奇异值分解，设矩阵为A(m x n). 奇异值分解将计算三个矩阵，分别是U,S,V
     * 它们满足 A ~= U * S * V', S包含了设定的k个奇异值，U，V为相应的奇异值向量
     */
    //   svd: org.apache.spark.mllib.linalg.SingularValueDecomposition[org.apache.spark.mllib.linalg.distributed.RowMatrix,org.apache.spark.mllib.linalg.Matrix] = 
    //SingularValueDecomposition(org.apache.spark.mllib.linalg.distributed.RowMatrix@688884e,[13.011193721236575,0.8419251442105343,7.793650306633694E-8],-0.2830233037672786  -0.7873358937103356  -0.5230588083704528  
    //-0.4132328277901395  -0.3594977469144485  0.5762839813994667   
    //-0.5434423518130005  0.06834039988143598  0.4166084623124157   
    //-0.6736518758358616  0.4961785466773299   -0.4698336353414313  )
    var svd: SingularValueDecomposition[RowMatrix, Matrix] = rowMatirx.computeSVD(3, true)

    //矩阵相乘积操作
    var multiplyMatrix: RowMatrix = rowMatirx.multiply(Matrices.dense(4, 1, Array(1, 2, 3, 4)))
  }
}