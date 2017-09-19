package org.apache.spark.examples.mllib

import org.apache.spark.mllib.stat.test.ChiSqTestResult
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
/**
 * 将五角星的5个角分别标记为1,2,3,4,5。现在旋转若干次五角星,记录每个角指向自己的次数。
 * 第一个的结果为(1,7,2,3,18),第二个五角星的结果为(7,8,6,7,9)。
 * 现做出虚无假设：五角星的每个角指向自己的概率是相同的。
 */
object ChiSqTestDome {
  def main(args: Array[String]) {
    val vec1 = Vectors.dense(1, 7, 2, 3, 18)//31次
    val vec2 = Vectors.dense(7, 8, 6, 7, 9)//73次
    val goodnessOfFitTestResult1 = Statistics.chiSqTest(vec1)
    val goodnessOfFitTestResult2 = Statistics.chiSqTest(vec2)
    println(goodnessOfFitTestResult1)
    println(goodnessOfFitTestResult2)
   
    /**
     * Chi squared test summary:
      method: pearson
      degrees of freedom = 4        自由度
      statistic = 31.41935483870968 标准差
      pValue = 2.513864414077638E-6 假定值、假设机率
      Very strong presumption against null hypothesis: observed follows the same distribution as expected..
      Chi squared test summary:
      method: pearson 
      degrees of freedom = 4          自由度
      statistic = 0.7027027027027026  标准差
      pValue = 0.9509952049458091     假定值、假设机率
      No presumption against null hypothesis: observed follows the same distribution as expected..     
     * 解读
			这里有5个维度的数据,自由度为4。根据均匀分布,每个维度出现的概率应该是1/5=0.2。
			而根据测试数据来看,这5个值出现的次数是1,7,2,3,18,显然是不符合概率都是0.2的。
			在一次实验中出现这样的分布的可能性即为p-value的值(2.5138644141226737E-6),
			这在一次实验中是不可能出现了,若出现了我们则有足够的理由认为假设是不成立的。
			所以拒绝了虚无假设(observed follows the same distribution as expected),
			即测试数据不满足均匀分布（这个五角星可能由于质量分布的原因导致这样的结果）。
			而另一组数据(7,8,6,7,9)则更符合等概率的出现,所以接受虚无假设。
     */
  }
}
