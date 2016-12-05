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

package org.apache.spark.sql.sources

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.datasources.ResolvedDataSource
  //解析数据源套件
class ResolvedDataSourceSuite extends SparkFunSuite {

  test("jdbc") {
    assert(
        //解析JDBC数据源
      ResolvedDataSource.lookupDataSource("jdbc") ===
        //默认使用jdbc.DefaultSource类型
      classOf[org.apache.spark.sql.execution.datasources.jdbc.DefaultSource])
    assert(
      ResolvedDataSource.lookupDataSource("org.apache.spark.sql.execution.datasources.jdbc") ===
      classOf[org.apache.spark.sql.execution.datasources.jdbc.DefaultSource])
    assert(
      ResolvedDataSource.lookupDataSource("org.apache.spark.sql.jdbc") ===
        classOf[org.apache.spark.sql.execution.datasources.jdbc.DefaultSource])
  }

  test("json") {
    assert(
        //解析json数据源
      ResolvedDataSource.lookupDataSource("json") ===
         //默认使用json.DefaultSource类型
      classOf[org.apache.spark.sql.execution.datasources.json.DefaultSource])
    assert(
      ResolvedDataSource.lookupDataSource("org.apache.spark.sql.execution.datasources.json") ===
        classOf[org.apache.spark.sql.execution.datasources.json.DefaultSource])
    assert(
      ResolvedDataSource.lookupDataSource("org.apache.spark.sql.json") ===
        classOf[org.apache.spark.sql.execution.datasources.json.DefaultSource])
  }

  test("parquet") {
    assert(
        //解析parquet数据源
      ResolvedDataSource.lookupDataSource("parquet") ===
        //默认使用parquet.DefaultSource类型
      classOf[org.apache.spark.sql.execution.datasources.parquet.DefaultSource])
    assert(
      ResolvedDataSource.lookupDataSource("org.apache.spark.sql.execution.datasources.parquet") ===
        classOf[org.apache.spark.sql.execution.datasources.parquet.DefaultSource])
    assert(
      ResolvedDataSource.lookupDataSource("org.apache.spark.sql.parquet") ===
        classOf[org.apache.spark.sql.execution.datasources.parquet.DefaultSource])
  }
}
