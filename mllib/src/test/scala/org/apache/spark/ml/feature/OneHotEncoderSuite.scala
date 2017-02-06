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

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.attribute.{AttributeGroup, BinaryAttribute, NominalAttribute}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
/**
 * one-hot一个有N个不同取值的类别特征可以变成N个数值特征,变换后的每个数值型特征的取值为0或1,
 * 在N个特征中,有且只有一个特征值为1,其他特征值都为0
 */
class OneHotEncoderSuite extends SparkFunSuite with MLlibTestSparkContext {

  def stringIndexed(): DataFrame = {
    val data = sc.parallelize(Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c")), 2)  
    val df = sqlContext.createDataFrame(data).toDF("id", "label")
      /**
      +---+-----+
      | id|label|
      +---+-----+
      |  0|    a|
      |  1|    b|
      |  2|    c|
      |  3|    a|
      |  4|    a|
      |  5|    c|
      +---+-----+
     */
    //a 3个,b 1个,c 2个
    df.show()
    //StringIndexer 按照 Label 出现的频次对其进行序列编码,字符串分类按出现次数排序索引0(a),2(b),1(c),
    val indexer = new StringIndexer().setInputCol("label").setOutputCol("labelIndex").fit(df)
    /**
     * 按照 Label 出现的频次对其进行序列编码 [0.0,a,0] a的编码是0,第二个0是ID,b编码是2,c的编码是1
     * 0是出现频次最多的,a 3个,b 1个,c 2个
      +---+-----+----------+
      | id|label|labelIndex|
      +---+-----+----------+
      |  0|    a|       0.0|
      |  1|    b|       2.0|
      |  2|    c|       1.0|
      |  3|    a|       0.0|
      |  4|    a|       0.0|
      |  5|    c|       1.0|
      +---+-----+----------+*/
     //transform()方法将DataFrame转化为另外一个DataFrame的算法
    indexer.transform(df).show()
    indexer.transform(df)
  }

  test("params") {//参数
    ParamsSuite.checkParams(new OneHotEncoder)
  }

  test("OneHotEncoder dropLast = false") {//
    val transformed = stringIndexed()
    // OneHotEncoder独热编码对于每一个特征,如果它有m个可能值,那么经过独热编码后,就变成m个二元特征,数据会变成稀疏
    val encoder = new OneHotEncoder().setInputCol("labelIndex").setOutputCol("labelVec").setDropLast(false)
    //transform主要是用来把 一个 DataFrame 转换成另一个 DataFrame     
    val encoded = encoder.transform(transformed)
    /**
     * 即3,[0],[1.0]共三个数据项,第1个为激活
      +---+-----+----------+-------------+
      | id|label|labelIndex|     labelVec|
      +---+-----+----------+-------------+
      |  0|    a|       0.0|(3,[0],[1.0])|
      |  1|    b|       2.0|(3,[2],[1.0])|
      |  2|    c|       1.0|(3,[1],[1.0])|
      |  3|    a|       0.0|(3,[0],[1.0])|
      |  4|    a|       0.0|(3,[0],[1.0])|
      |  5|    c|       1.0|(3,[1],[1.0])|
      +---+-----+----------+-------------+*/
    encoded.show()
 
    val output = encoded.select("id", "labelVec").map { r =>
      val vec = r.getAs[Vector](1)//转换成向量
      (r.getInt(0), vec(0), vec(1), vec(2))//默认转换成密集重向量
    }.collect().toSet
    // a -> 0, b -> 2, c -> 1 表示意思[a, c, b]
    //StringIndexer 按照 Label出现的频次对其进行升序编码,a编码0(3次现),b编码2次现,c编码1次现,
    //采用One-Hot编码的方式对上述的样本["a","b","c"]编码,a对应[1.0, 0.0, 0.0],b对应[0.0, 0.0, 1.0],b对应[0.0, 1.0, 0.0]
    //(0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c")
    val expected = Set((0, 1.0, 0.0, 0.0), (1, 0.0, 0.0, 1.0), (2, 0.0, 1.0, 0.0),
      (3, 1.0, 0.0, 0.0), (4, 1.0, 0.0, 0.0), (5, 0.0, 1.0, 0.0))
    assert(output === expected)
  }
  //删除最后
  test("OneHotEncoder dropLast = true") {
    val transformed = stringIndexed()
    val encoder = new OneHotEncoder()
      .setInputCol("labelIndex")
      .setOutputCol("labelVec")
       //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val encoded = encoder.transform(transformed)
    /**
    +---+-----+----------+-------------+
    | id|label|labelIndex|     labelVec|
    +---+-----+----------+-------------+
    |  0|    a|       0.0|(2,[0],[1.0])|
    |  1|    b|       2.0|    (2,[],[])|
    |  2|    c|       1.0|(2,[1],[1.0])|
    |  3|    a|       0.0|(2,[0],[1.0])|
    |  4|    a|       0.0|(2,[0],[1.0])|
    |  5|    c|       1.0|(2,[1],[1.0])|
    +---+-----+----------+-------------+*/
      encoded.show()
    val output = encoded.select("id", "labelVec").map { r =>
      val vec = r.getAs[Vector](1)//获得向量
      (r.getInt(0), vec(0), vec(1))//默认转换成密集向量
    }.collect().toSet
    // a -> 0, b -> 2, c -> 1 [a, c, b],删除最后一位编码
    //(0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c")
    val expected = Set((0, 1.0, 0.0), (1, 0.0, 0.0), (2, 0.0, 1.0),
      (3, 1.0, 0.0), (4, 1.0, 0.0), (5, 0.0, 1.0))
    assert(output === expected)
  }

  test("input column with ML attribute") {//具有输入列ML属性
    //输入列
    val attr = NominalAttribute.defaultAttr.withValues("small", "medium", "large")
    val df = sqlContext.createDataFrame(Seq(0.0, 1.0, 2.0, 1.0).map(Tuple1.apply)).toDF("size")
      .select(col("size").as("size", attr.toMetadata()))
      /**
      +----+
      |size|
      +----+
      | 0.0|
      | 1.0|
      | 2.0|
      | 1.0|
      +----+*/
      df.show()
    val encoder = new OneHotEncoder().setInputCol("size").setOutputCol("encoded")
     //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val output = encoder.transform(df)
    /**
      +----+-------------+
      |size|      encoded|
      +----+-------------+
      | 0.0|(2,[0],[1.0])|
      | 1.0|(2,[1],[1.0])|
      | 2.0|    (2,[],[])|
      | 1.0|(2,[1],[1.0])|
      +----+-------------+*/
    output.show()
    val group = AttributeGroup.fromStructField(output.schema("encoded"))
    assert(group.size === 2)
    assert(group.getAttr(0) === BinaryAttribute.defaultAttr.withName("small").withIndex(0))
    assert(group.getAttr(1) === BinaryAttribute.defaultAttr.withName("medium").withIndex(1))
  }

  test("input column without ML attribute") {//具输入列没有ML属性
    val df = sqlContext.createDataFrame(Seq(0.0, 1.0, 2.0, 1.0).map(Tuple1.apply)).toDF("index")
    val encoder = new OneHotEncoder().setInputCol("index").setOutputCol("encoded")
     //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val output = encoder.transform(df)
    val group = AttributeGroup.fromStructField(output.schema("encoded"))
    assert(group.size === 2)
    assert(group.getAttr(0) === BinaryAttribute.defaultAttr.withName("0").withIndex(0))
    assert(group.getAttr(1) === BinaryAttribute.defaultAttr.withName("1").withIndex(1))
  }
}
