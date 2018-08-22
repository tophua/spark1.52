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
package org.apache.spark.ml.feature

import org.apache.spark.annotation.Experimental
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.{Identifiable, SchemaUtils}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.mllib.linalg.{VectorUDT, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.util.collection.OpenHashMap

/**
 * Params for [[CountVectorizer]] and [[CountVectorizerModel]].
  * [[CountVectorizer]]和[[CountVectorizerModel]]的参数
 */
private[feature] trait CountVectorizerParams extends Params with HasInputCol with HasOutputCol {

  /**
   * Max size of the vocabulary.
    * 词汇量的最大大小
   * CountVectorizer will build a vocabulary that only considers the top
   * vocabSize terms ordered by term frequency across the corpus.
   * CountVectorizer将构建一个词汇表,该词汇表仅考虑按语料库中的术语频率排序的顶级vocabSize术语
   * Default: 2^18^
   * @group param
   */
  val vocabSize: IntParam =
    new IntParam(this, "vocabSize", "max size of the vocabulary", ParamValidators.gt(0))

  /** @group getParam */
  def getVocabSize: Int = $(vocabSize)

  /**
   * Specifies the minimum number of different documents a term must appear in to be included
   * in the vocabulary.
    * 指定术语必须包含在词汇表中的最小不同文档数
   * If this is an integer >= 1, this specifies the number of documents the term must appear in;
   * if this is a double in [0,1), then this specifies the fraction of documents.
   *
   * Default: 1
   * @group param
   */
  val minDF: DoubleParam = new DoubleParam(this, "minDF", "Specifies the minimum number of" +
    " different documents a term must appear in to be included in the vocabulary." +
    " If this is an integer >= 1, this specifies the number of documents the term must" +
    " appear in; if this is a double in [0,1), then this specifies the fraction of documents.",
    ParamValidators.gtEq(0.0))

  /** @group getParam */
  def getMinDF: Double = $(minDF)

  /** Validates and transforms the input schema. 验证并转换输入架构*/
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), new ArrayType(StringType, true))
    SchemaUtils.appendColumn(schema, $(outputCol), new VectorUDT)
  }

  /**
   * Filter to ignore rare words in a document. For each document, terms with
   * frequency/count less than the given threshold are ignored.
    * 过滤以忽略文档中的罕见单词。对于每个文档,忽略频率/计数小于给定阈值的术语
   * If this is an integer >= 1, then this specifies a count (of times the term must appear
   * in the document);
    * 如果这是一个> = 1的整数,则指定一个计数（该术语必须出现在文档中的次数）
   * if this is a double in [0,1), then this specifies a fraction (out of the document's token
   * count).
   * 如果这是[0,1]中的双精度数,那么这将指定一个分数(超出文档的令牌数)
   * Note that the parameter is only used in transform of [[CountVectorizerModel]] and does not
   * affect fitting.
   *
   * Default: 1
   * @group param
   */
  val minTF: DoubleParam = new DoubleParam(this, "minTF", "Filter to ignore rare words in" +
    " a document. For each document, terms with frequency/count less than the given threshold are" +
    " ignored. If this is an integer >= 1, then this specifies a count (of times the term must" +
    " appear in the document); if this is a double in [0,1), then this specifies a fraction (out" +
    " of the document's token count). Note that the parameter is only used in transform of" +
    " CountVectorizerModel and does not affect fitting.", ParamValidators.gtEq(0.0))

  setDefault(minTF -> 1)

  /** @group getParam */
  def getMinTF: Double = $(minTF)
}

/**
 * :: Experimental ::
 * Extracts a vocabulary from document collections and generates a [[CountVectorizerModel]].
  * 从文档集合中提取词汇表并生成[[CountVectorizerModel]]
 */
@Experimental
class CountVectorizer(override val uid: String)
  extends Estimator[CountVectorizerModel] with CountVectorizerParams {

  def this() = this(Identifiable.randomUID("cntVec"))

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setVocabSize(value: Int): this.type = set(vocabSize, value)

  /** @group setParam */
  def setMinDF(value: Double): this.type = set(minDF, value)

  /** @group setParam */
  def setMinTF(value: Double): this.type = set(minTF, value)

  setDefault(vocabSize -> (1 << 18), minDF -> 1)

  override def fit(dataset: DataFrame): CountVectorizerModel = {
    transformSchema(dataset.schema, logging = true)
    val vocSize = $(vocabSize)
    val input = dataset.select($(inputCol)).map(_.getAs[Seq[String]](0))
    val minDf = if ($(minDF) >= 1.0) {
      $(minDF)
    } else {
      $(minDF) * input.cache().count()
    }
    val wordCounts: RDD[(String, Long)] = input.flatMap { case (tokens) =>
      val wc = new OpenHashMap[String, Long]
      tokens.foreach { w =>
        wc.changeValue(w, 1L, _ + 1L)
      }
      wc.map { case (word, count) => (word, (count, 1)) }
    }.reduceByKey { case ((wc1, df1), (wc2, df2)) =>
      (wc1 + wc2, df1 + df2)
    }.filter { case (word, (wc, df)) =>
      df >= minDf
    }.map { case (word, (count, dfCount)) =>
      (word, count)
    }.cache()
    val fullVocabSize = wordCounts.count()
    val vocab: Array[String] = {
      val tmpSortedWC: Array[(String, Long)] = if (fullVocabSize <= vocSize) {
        // Use all terms
        wordCounts.collect().sortBy(-_._2)
      } else {
        // Sort terms to select vocab
        wordCounts.sortBy(_._2, ascending = false).take(vocSize)
      }
      tmpSortedWC.map(_._1)
    }

    require(vocab.length > 0, "The vocabulary size should be > 0. Lower minDF as necessary.")
    copyValues(new CountVectorizerModel(uid, vocab).setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): CountVectorizer = defaultCopy(extra)
}

/**
 * :: Experimental ::
 * Converts a text document to a sparse vector of token counts.
  * 将文本文档转换为令牌计数的稀疏向量
 * @param vocabulary An Array over terms. Only the terms in the vocabulary will be counted.
 */
@Experimental
class CountVectorizerModel(override val uid: String, val vocabulary: Array[String])
  extends Model[CountVectorizerModel] with CountVectorizerParams {

  def this(vocabulary: Array[String]) = {
    this(Identifiable.randomUID("cntVecModel"), vocabulary)
    set(vocabSize, vocabulary.length)
  }

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setMinTF(value: Double): this.type = set(minTF, value)

  /** Dictionary created from [[vocabulary]] and its indices, broadcast once for [[transform()]]
    * 从[词汇]及其索引创建的词典，为[transform（）]广播一次]*/
  private var broadcastDict: Option[Broadcast[Map[String, Int]]] = None

  override def transform(dataset: DataFrame): DataFrame = {
    if (broadcastDict.isEmpty) {
      val dict = vocabulary.zipWithIndex.toMap
      broadcastDict = Some(dataset.sqlContext.sparkContext.broadcast(dict))
    }
    val dictBr = broadcastDict.get
    val minTf = $(minTF)
    val vectorizer = udf { (document: Seq[String]) =>
      val termCounts = new OpenHashMap[Int, Double]
      var tokenCount = 0L
      document.foreach { term =>
        dictBr.value.get(term) match {
          case Some(index) => termCounts.changeValue(index, 1.0, _ + 1.0)
          case None => // ignore terms not in the vocabulary
        }
        tokenCount += 1
      }
      val effectiveMinTF = if (minTf >= 1.0) {
        minTf
      } else {
        tokenCount * minTf
      }
      Vectors.sparse(dictBr.value.size, termCounts.filter(_._2 >= effectiveMinTF).toSeq)
    }
    dataset.withColumn($(outputCol), vectorizer(col($(inputCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): CountVectorizerModel = {
    val copied = new CountVectorizerModel(uid, vocabulary).setParent(parent)
    copyValues(copied, extra)
  }
}
