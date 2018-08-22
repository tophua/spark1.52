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

package org.apache.spark.ml

import java.{util => ju}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.spark.Logging
import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
 * :: DeveloperApi ::
 * A stage in a pipeline, either an [[Estimator]] or a [[Transformer]].
 */
@DeveloperApi
abstract class PipelineStage extends Params with Logging {

  /**
   * :: DeveloperApi ::
   *
   * Derives the output schema from the input schema.
    * 从输入模式派生输出模式
   */
  @DeveloperApi
  def transformSchema(schema: StructType): StructType

  /**
   * :: DeveloperApi ::
   *
   * Derives the output schema from the input schema and parameters, optionally with logging.
    * 从输入模式和参数派生输出模式,可选择使用日志记录
   *
   * This should be optimistic.  If it is unclear whether the schema will be valid, then it should
   * be assumed valid until proven otherwise.
    * 这应该是乐观的,如果不清楚架构是否有效,则应该假设它有效,直到另有证明为止
   */
  @DeveloperApi
  protected def transformSchema(
      schema: StructType,
      logging: Boolean): StructType = {
    if (logging) {
      logDebug(s"Input schema: ${schema.json}")
    }
    val outputSchema = transformSchema(schema)
    if (logging) {
      logDebug(s"Expected output schema: ${outputSchema.json}")
    }
    outputSchema
  }

  override def copy(extra: ParamMap): PipelineStage
}

/**
 * :: Experimental ::
 * A simple pipeline, which acts as an estimator. A Pipeline consists of a sequence of stages, each
 * of which is either an [[Estimator]] or a [[Transformer]]. When [[Pipeline#fit]] is called, the
 * stages are executed in order. If a stage is an [[Estimator]], its [[Estimator#fit]] method will
 * be called on the input dataset to fit a model. Then the model, which is a transformer, will be
 * used to transform the dataset as the input to the next stage. If a stage is a [[Transformer]],
 * its [[Transformer#transform]] method will be called to produce the dataset for the next stage.
 * The fitted model from a [[Pipeline]] is an [[PipelineModel]], which consists of fitted models and
 * transformers, corresponding to the pipeline stages. If there are no stages, the pipeline acts as
 * an identity transformer.
 */
@Experimental
class Pipeline(override val uid: String) extends Estimator[PipelineModel] {

  def this() = this(Identifiable.randomUID("pipeline"))

  /**
   * param for pipeline stages
    * 管道阶段的参数
   * @group param
   */
  val stages: Param[Array[PipelineStage]] = new Param(this, "stages", "stages of the pipeline")

  /** @group setParam */
  def setStages(value: Array[PipelineStage]): this.type = { set(stages, value); this }

  // Below, we clone stages so that modifications to the list of stages will not change
  // the Param value in the Pipeline.
  //下面，我们克隆阶段，以便修改阶段列表不会更改管道中的Param值
  /** @group getParam */
  def getStages: Array[PipelineStage] = $(stages).clone()

  override def validateParams(): Unit = {
    super.validateParams()
    $(stages).foreach(_.validateParams())
  }

  /**
   * Fits the pipeline to the input dataset with additional parameters. If a stage is an
   * [[Estimator]], its [[Estimator#fit]] method will be called on the input dataset to fit a model.
   * Then the model, which is a transformer, will be used to transform the dataset as the input to
   * the next stage. If a stage is a [[Transformer]], its [[Transformer#transform]] method will be
   * called to produce the dataset for the next stage. The fitted model from a [[Pipeline]] is an
   * [[PipelineModel]], which consists of fitted models and transformers, corresponding to the
   * pipeline stages. If there are no stages, the output model acts as an identity transformer.
    *
    * 使用其他参数使管道适合输入数据集,如果舞台是[[Estimator]],则会在输入数据集上调用其[[Estimator＃fit]]方法以适合模型。
    * 然后,模型（变换器）将用于将数据集转换为下一阶段的输入,如果阶段是[[Transformer]],
    * 则将调用其[[Transformer＃transform]]方法以生成下一阶段的数据集,来自[[Pipeline]]的拟合模型是[[PipelineModel]],
    * 它由适合的模型和变换器组成,对应于管道阶段。 如果没有阶段,则输出模型充当身份变换器
   *
   * @param dataset input dataset
   * @return fitted pipeline
   */
  override def fit(dataset: DataFrame): PipelineModel = {
    transformSchema(dataset.schema, logging = true)
    val theStages = $(stages)
    // Search for the last estimator.
    var indexOfLastEstimator = -1
    theStages.view.zipWithIndex.foreach { case (stage, index) =>
      stage match {
        case _: Estimator[_] =>
          indexOfLastEstimator = index
        case _ =>
      }
    }
    var curDataset = dataset
    val transformers = ListBuffer.empty[Transformer]
    theStages.view.zipWithIndex.foreach { case (stage, index) =>
      if (index <= indexOfLastEstimator) {
        val transformer = stage match {
          case estimator: Estimator[_] =>
            estimator.fit(curDataset)
          case t: Transformer =>
            t
          case _ =>
            throw new IllegalArgumentException(
              s"Do not support stage $stage of type ${stage.getClass}")
        }
        if (index < indexOfLastEstimator) {
          curDataset = transformer.transform(curDataset)
        }
        transformers += transformer
      } else {
        transformers += stage.asInstanceOf[Transformer]
      }
    }

    new PipelineModel(uid, transformers.toArray).setParent(this)
  }

  override def copy(extra: ParamMap): Pipeline = {
    val map = extractParamMap(extra)
    val newStages = map(stages).map(_.copy(extra))
    new Pipeline().setStages(newStages)
  }

  override def transformSchema(schema: StructType): StructType = {
    val theStages = $(stages)
    require(theStages.toSet.size == theStages.length,
      "Cannot have duplicate components in a pipeline.")
    theStages.foldLeft(schema)((cur, stage) => stage.transformSchema(cur))
  }
}

/**
 * :: Experimental ::
 * Represents a fitted pipeline.
  * 表示适合的管道
 */
@Experimental
class PipelineModel private[ml] (
    override val uid: String,
    val stages: Array[Transformer])
  extends Model[PipelineModel] with Logging {

  /** A Java/Python-friendly auxiliary constructor.
    * 一个Java / Python友好的辅助构造函数*/
  private[ml] def this(uid: String, stages: ju.List[Transformer]) = {
    this(uid, stages.asScala.toArray)
  }

  override def validateParams(): Unit = {
    super.validateParams()
    stages.foreach(_.validateParams())
  }

  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    stages.foldLeft(dataset)((cur, transformer) => transformer.transform(cur))
  }

  override def transformSchema(schema: StructType): StructType = {
    stages.foldLeft(schema)((cur, transformer) => transformer.transformSchema(cur))
  }

  override def copy(extra: ParamMap): PipelineModel = {
    new PipelineModel(uid, stages.map(_.copy(extra))).setParent(parent)
  }
}
