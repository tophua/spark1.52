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
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.Row
// $example off$
import org.apache.spark.sql.Row
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SQLContext, DataFrame}

object PipelineExample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PipelineExample").setMaster("local[4]")
    val sc = new SparkContext(conf)
  
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    // $example on$
    // Prepare training documents from a list of (id, text, label) tuples.
    //准备训练文档来自一个列表(ID、文本,标签)元组
    val training = sqlContext.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0)
    )).toDF("id", "text", "label")

    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    //HashingTF使用每个单词对所需向量的长度S取模得出的哈希值,把所有单词映射到一个0到S-1之间的数字上
    val hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
    //逻辑回归
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.01)
       //PipeLine:将多个DataFrame和Estimator算法串成一个特定的ML Wolkflow
       //一个 Pipeline在结构上会包含一个或多个 PipelineStage,每一个 PipelineStage 都会完成一个任务
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))

    // Fit the pipeline to training documents.
    //fit()方法将DataFrame转化为一个Transformer的算法
    val model = pipeline.fit(training)
    /*
    // Now we can optionally save the fitted pipeline to disk
     * 现在我们可以选择性地将安装的管道保存到磁盘上    
    model.write.overwrite().save("/tmp/spark-logistic-regression-model")

    // We can also save this unfit pipeline to disk
     * 我们也可以保存这个不适合管道到磁盘     
    pipeline.write.overwrite().save("/tmp/unfit-lr-model")

    // And load it back in during production
     * 在生产过程中加载     
    val sameModel = PipelineModel.load("/tmp/spark-logistic-regression-model")*/

    // Prepare test documents, which are unlabeled (id, text) tuples.
    //准备测试文档,这是未标记的(ID、文本)的元组
    val test = sqlContext.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "mapreduce spark"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

    // Make predictions on test documents.
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    /**
    +---+---------------+------------------+--------------------+--------------------+--------------------+----------+
    | id|           text|             words|            features|       rawPrediction|         probability|prediction|
    +---+---------------+------------------+--------------------+--------------------+--------------------+----------+
    |  4|    spark i j k|  [spark, i, j, k]|(1000,[105,106,10...|[0.16293291377552...|[0.54064335448514...|       0.0|
    |  5|          l m n|         [l, m, n]|(1000,[108,109,11...|[2.64074492867998...|[0.93343826273832...|       0.0|
    |  6|mapreduce spark|[mapreduce, spark]|(1000,[365,810],[...|[1.26512849874491...|[0.77990768682039...|       0.0|
    |  7|  apache hadoop|  [apache, hadoop]|(1000,[269,894],[...|[3.74294051364937...|[0.97686361395183...|       0.0|
    +---+---------------+------------------+--------------------+--------------------+--------------------+----------+*/
    model.transform(test).show()
    model.transform(test)
    //probability算法预测结果的存储列的名称
    //probability类别预测结果的条件概率值存储列的名称
      .select("id", "text", "probability", "prediction")
      .collect()
      .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
        /**
         *(4, spark i j k) --> prob=[0.5406433544851448,0.4593566455148551], prediction=0.0
         *(5, l m n) --> prob=[0.9334382627383266,0.06656173726167328], prediction=0.0
         *(6, mapreduce spark) --> prob=[0.7799076868203906,0.22009231317960942], prediction=0.0
         *(7, apache hadoop) --> prob=[0.9768636139518305,0.023136386048169463], prediction=0.0
         */
        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
      }
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
