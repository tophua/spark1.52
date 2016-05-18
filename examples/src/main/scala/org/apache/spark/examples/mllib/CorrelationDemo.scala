package org.apache.spark.examples.mllib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
/**
 * Correlation 相关性分析
 * PearsonCorrelation在Spark中是私有成员，不能直接访问，使用时仍然是通过Statistics对象进行
 */
object CorrelationDemo {
  def main(args: Array[String]) {
    //val sparkConf = new SparkConf().setMast("local[2]").setAppName("SparkHdfsLR")

    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd1: RDD[Double] = sc.parallelize(Array(11.0, 21.0, 13.0, 14.0))
    val rdd2: RDD[Double] = sc.parallelize(Array(11.0, 20.0, 13.0, 16.0))
    //两个rdd间的相关性
    //返回值：correlation: Double = 0.959034501397483
    //[-1, 1]，值越接近于1，其相关度越高
    val correlation: Double = Statistics.corr(rdd1, rdd2, "pearson")

    val rdd3 = sc.parallelize(
      Array(
        Array(1.0, 2.0, 3.0, 4.0),
        Array(2.0, 3.0, 4.0, 5.0),
        Array(3.0, 4.0, 5.0, 6.0))).map(f => Vectors.dense(f))
    //correlation3: org.apache.spark.mllib.linalg.Matrix = 
    //1.0  1.0  1.0  1.0  
    //1.0  1.0  1.0  1.0  
    //1.0  1.0  1.0  1.0  
    //1.0  1.0  1.0  1.0  
    val correlation3: Matrix = Statistics.corr(rdd3, "pearson")
    /**
     * 假设某工厂通过随机抽样得到考试成绩与产量之间的关系数据如下：
     * 这里写图片描述
     * 直观地看，成绩越高产量越高，如果使用pearson相关系数，将得到如下结果：
     */
    val rdd4: RDD[Double] = sc.parallelize(Array(50.0, 60.0, 70.0, 80.0, 90.0, 95.0))
    val rdd5: RDD[Double] = sc.parallelize(Array(500.0, 510.0, 530.0, 580.0, 560, 1000))
    //执行结果为:
    //correlation4: Double = 0.6915716600436548
    //但其实从我们观察的数据来看，它们应该是高度相关的，虽然0.69也一定程度地反应了数据间的相关性
    val correlation4: Double = Statistics.corr(rdd4, rdd5, "pearson")
    /**
     * *
     * 如表中的第四、第五列数据，通过将成绩和产量替换成等级，那它们之间的相关度会明显提高，这样的话表达能力更强，如下列代码所示：
     */
    //采用spearman相关系数
    //执行结果：
    //correlation5: Double = 0.9428571428571412
    val correlation5: Double = Statistics.corr(rdd4, rdd5, "spearman")
    //从上面的执行结果来看，相关性从pearson的值0.6915716600436548提高到了0.9428571428571412。由于利用的等级相关，
    //因而spearman相关性分析也称为spearman等级相关分析或等级差数法，但需要注意的是spearman相关性分析方法涉及到等级的排序问题，
    //在分布式环境下的排序可能会涉及到大量的网络IO操作，算法效率不是特别高。
  }
}