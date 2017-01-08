/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples.ml

// $example on$
import org.apache.spark.ml.feature.Bucketizer
// $example off$
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SQLContext, DataFrame}
/**
 * 分箱(分段处理):将连续数值转换为离散类别
 * 比如特征是年龄,是一个连续数值,需要将其转换为离散类别(未成年人、青年人、中年人、老年人),就要用到Bucketizer了
 */
object BucketizerExample {
  def main(args: Array[String]): Unit = {
   val conf = new SparkConf().setAppName("BucketizerExample").setMaster("local[4]")
    val sc = new SparkContext(conf)
  
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    // $example on$
    /**
    * 分类的标准是自己定义的,在Spark中为split参数,定义如下：
		*	double[] splits = {0, 18, 35,50， Double.PositiveInfinity}
		* 将数值年龄分为四类0-18，18-35，35-50，55+四个段
    */
    val splits = Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity)

    val data = Array(-0.5, -0.3, 0.0, 0.5,0.6)//数据
    val dataFrame = sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val bucketizer = new Bucketizer()
      .setInputCol("features")//输入字段
      .setOutputCol("bucketedFeatures")//输出字段
      .setSplits(splits)//设置分段标准,注意分隔小于边界值

    // Transform original data into its bucket index.
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val bucketedData = bucketizer.transform(dataFrame)
    /**
    +--------+----------------+
    |features|bucketedFeatures|
    +--------+----------------+
    |    -0.5|             1.0|
    |    -0.3|             1.0|
    |     0.0|             2.0|
    |     0.5|             3.0|
    |     0.6|             3.0|
    +--------+----------------+
     */
    bucketedData.show()
    // $example off$
    sc.stop()
  }
}
// scalastyle:on println

