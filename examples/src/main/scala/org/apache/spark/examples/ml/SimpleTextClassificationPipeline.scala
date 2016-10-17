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

import scala.beans.BeanInfo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.{Row, SQLContext}

@BeanInfo
case class LabeledDocument(id: Long, text: String, label: Double)

@BeanInfo
case class Document(id: Long, text: String)

  /**
   * 简单分类分类示例
   * A simple text classification pipeline that recognizes "spark" from input text. This is to show
   * 一个简单的文本分类管道，从“Spark”输入文本中识别
   * how to create and configure an ML pipeline. Run with
   * 这是显示如何创建和配置一个ML的管道
   * {{{
   * bin/run-example ml.SimpleTextClassificationPipeline
   * }}}
   */
object SimpleTextClassificationPipeline {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SimpleTextClassificationPipeline").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // Prepare training documents, which are labeled.
    //准备训练文档,这些文件都是有标签
    val training = sc.parallelize(Seq(
      //文档ID,内容,标号
      LabeledDocument(0L, "a b c d e spark", 1.0),
      LabeledDocument(1L, "b d", 0.0),
      LabeledDocument(2L, "spark f g h", 1.0),
      LabeledDocument(3L, "hadoop mapreduce", 0.0)))

    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    //配置ML管理,其中包括三个阶段
    //使用将文本拆分成单词
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    //特征提取和转换 TF-IDF
    val hashingTF = new HashingTF()
      .setNumFeatures(1000)//设置特征值
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")//text=spark hadoop spark,features=(1000,[269,365],[1.0,2.0]) //2代表spark出现2次
    //把词频作为输入特征创建逻辑回归分类器
    val lr = new LogisticRegression()
      .setMaxIter(10)//最大迭代次数
      .setRegParam(0.001)
    //将这些操作合并到一个pipeline中,让pipeline实际执行从输入训练数据中构造模型的工作
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))

    // Fit the pipeline to training documents.
    //将管道安装到训练文档
    //隐式转换为schemaRDD
    val model = pipeline.fit(training.toDF())

    // Prepare test documents, which are unlabeled.
    //准备测试文档,这些文件是未标记的
    //将模型用于新文档的分类,
    /**
      LabeledDocument(0L, "a b c d e spark", 1.0),
      LabeledDocument(1L, "b d",0.0),
      LabeledDocument(2L, "spark f g h", 1.0),
      LabeledDocument(3L, "hadoop mapreduce", 0.0)))
     */
    val test = sc.parallelize(Seq(
      Document(4L, "spark i j k"),
      Document(5L, "l m n  i k"),
      Document(6L, "spark hadoop spark"),
      Document(7L, "apache hadoop")))

    // Make predictions on test documents.
     //注意Model实际上是一个包含所有转换逻辑的pipeline,而不是一个对分类的调用
    model.transform(test.toDF())
      .select("id", "text","features", "probability", "prediction")
      .collect()
      .foreach { case Row(id: Long, text: String,  features: Vector, prob: Vector, prediction: Double) =>
        //文档ID,text文本,probability概率,prediction 预测分类
        println(s"($id, $text) --> prob=$prob, prediction=$prediction,text=$text,features=$features")
      }
   
   /**
    * (4, spark i j k) --> prob=[0.1596407738787411,0.8403592261212589], prediction=1.0
    * (5, l m n) --> prob=[0.8378325685476612,0.16216743145233883], prediction=0.0
    * (6, spark hadoop spark) --> prob=[0.0692663313297627,0.9307336686702373], prediction=1.0
    * (7, apache hadoop) --> prob=[0.9821575333444208,0.01784246665557917], prediction=0.0
    */

    sc.stop()
  }
}
// scalastyle:on println
