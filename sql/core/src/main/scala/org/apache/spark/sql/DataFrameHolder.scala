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

package org.apache.spark.sql

/**
 * A container for a [[DataFrame]], used for implicit conversions.
 * 一个容器为DataFrame,用于隐式转换。
 *
 * @since 1.3.0
 */
private[sql] case class DataFrameHolder(df: DataFrame) {

  // This is declared with parentheses to prevent the Scala compiler from treating
  //这是声明括号来防止Scala编译器处理,TODF然后调用这个应用在返回的数据框
  // `rdd.toDF("1")` as invoking this toDF and then apply on the returned DataFrame.
  //
  def toDF(): DataFrame = df

  def toDF(colNames: String*): DataFrame = df.toDF(colNames : _*)
}
