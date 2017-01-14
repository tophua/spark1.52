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

import scala.collection.JavaConverters._

import org.mockito.Matchers.{any, eq => meq}
import org.mockito.Mockito.when
import org.scalatest.mock.MockitoSugar.mock

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.MLTestingUtils
import org.apache.spark.sql.DataFrame
/**
 *PipeLine:将多个DataFrame和Estimator算法串成一个特定的ML Wolkflow
 */
class PipelineSuite extends SparkFunSuite {

  abstract class MyModel extends Model[MyModel]

  test("pipeline") {//管道
    val estimator0 = mock[Estimator[MyModel]]
    val model0 = mock[MyModel]
    val transformer1 = mock[Transformer]
    val estimator2 = mock[Estimator[MyModel]]
    val model2 = mock[MyModel]
    val transformer3 = mock[Transformer]
    val dataset0 = mock[DataFrame]
    val dataset1 = mock[DataFrame]
    val dataset2 = mock[DataFrame]
    val dataset3 = mock[DataFrame]
    val dataset4 = mock[DataFrame]

    when(estimator0.copy(any[ParamMap])).thenReturn(estimator0)
    when(model0.copy(any[ParamMap])).thenReturn(model0)
    when(transformer1.copy(any[ParamMap])).thenReturn(transformer1)
    when(estimator2.copy(any[ParamMap])).thenReturn(estimator2)
    when(model2.copy(any[ParamMap])).thenReturn(model2)
    when(transformer3.copy(any[ParamMap])).thenReturn(transformer3)
    //fit()方法将DataFrame转化为一个Transformer的算法
    when(estimator0.fit(meq(dataset0))).thenReturn(model0)
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    when(model0.transform(meq(dataset0))).thenReturn(dataset1)
    when(model0.parent).thenReturn(estimator0)
    when(transformer1.transform(meq(dataset1))).thenReturn(dataset2)
    //fit()方法将DataFrame转化为一个Transformer的算法
    when(estimator2.fit(meq(dataset2))).thenReturn(model2)
    when(model2.transform(meq(dataset2))).thenReturn(dataset3)
    when(model2.parent).thenReturn(estimator2)
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    when(transformer3.transform(meq(dataset3))).thenReturn(dataset4)
    //PipeLine:将多个DataFrame和Estimator算法串成一个特定的ML Wolkflow
   //一个 Pipeline在结构上会包含一个或多个 PipelineStage,每一个 PipelineStage 都会完成一个任务
    val pipeline = new Pipeline()
      .setStages(Array(estimator0, transformer1, estimator2, transformer3))
      //fit()方法将DataFrame转化为一个Transformer的算法
    val pipelineModel = pipeline.fit(dataset0)

    MLTestingUtils.checkCopy(pipelineModel)
    //获得管道个数
    assert(pipelineModel.stages.length === 4)
    assert(pipelineModel.stages(0).eq(model0))
    assert(pipelineModel.stages(1).eq(transformer1))
    assert(pipelineModel.stages(2).eq(model2))
    assert(pipelineModel.stages(3).eq(transformer3))
   //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val output = pipelineModel.transform(dataset0)
    assert(output.eq(dataset4))
  }

  test("pipeline with duplicate stages") {//重复阶段管道
    val estimator = mock[Estimator[MyModel]]
     //PipeLine:将多个DataFrame和Estimator算法串成一个特定的ML Wolkflow
     //一个 Pipeline在结构上会包含一个或多个 PipelineStage,每一个 PipelineStage 都会完成一个任务
    val pipeline = new Pipeline()
      .setStages(Array(estimator, estimator))
    val dataset = mock[DataFrame]
    intercept[IllegalArgumentException] {
     //fit()方法将DataFrame转化为一个Transformer的算法
      pipeline.fit(dataset)
    }
  }

  test("PipelineModel.copy") {//管道模型复制
    val hashingTF = new HashingTF()
      .setNumFeatures(100)
      //PipeLine:将多个DataFrame和Estimator算法串成一个特定的ML Wolkflow
    val model = new PipelineModel("pipeline", Array[Transformer](hashingTF))
    //管道模型复制
    val copied = model.copy(ParamMap(hashingTF.numFeatures -> 10))
    //获得管道HashingTF模型
    require(copied.stages(0).asInstanceOf[HashingTF].getNumFeatures === 10,
      "copy should handle extra stage params")
  }

  test("pipeline model constructors") {//管道模型构造器
    val transform0 = mock[Transformer]
    val model1 = mock[MyModel]

    val stages = Array(transform0, model1)
     //PipeLine:将多个DataFrame和Estimator算法串成一个特定的ML Wolkflow
    val pipelineModel0 = new PipelineModel("pipeline0", stages)
    assert(pipelineModel0.uid === "pipeline0")
    assert(pipelineModel0.stages === stages)

    val stagesAsList = stages.toList.asJava
     //PipeLine:将多个DataFrame和Estimator算法串成一个特定的ML Wolkflow
    val pipelineModel1 = new PipelineModel("pipeline1", stagesAsList)
    assert(pipelineModel1.uid === "pipeline1")
    assert(pipelineModel1.stages === stages)
  }
}
