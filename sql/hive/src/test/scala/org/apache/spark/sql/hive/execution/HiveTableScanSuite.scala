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

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.hive.test.TestHive.implicits._

import org.apache.spark.util.Utils

/**
  * Hive表扫描测试
  */
class HiveTableScanSuite extends HiveComparisonTest {

  createQueryTest("partition_based_table_scan_with_different_serde",
    """
      |CREATE TABLE part_scan_test (key STRING, value STRING) PARTITIONED BY (ds STRING)
      |ROW FORMAT SERDE
      |'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe'
      |STORED AS RCFILE;
      |
      |FROM src
      |INSERT INTO TABLE part_scan_test PARTITION (ds='2010-01-01')
      |SELECT 100,100 LIMIT 1;
      |
      |ALTER TABLE part_scan_test SET SERDE
      |'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe';
      |
      |FROM src INSERT INTO TABLE part_scan_test PARTITION (ds='2010-01-02')
      |SELECT 200,200 LIMIT 1;
      |
      |SELECT * from part_scan_test;
    """.stripMargin)

  // In unit test, kv1.txt is a small file and will be loaded as table src
  // Since the small file will be considered as a single split, we assume
  // Hive / SparkSQL HQL has the same output even for SORT BY
  //在单元测试中,kv1.txt是一个小文件,将作为表src加载由于小文件将被视为单个拆分,
  //我们假设Hive / SparkSQL HQL即使对于SORT BY也具有相同的输出
  createQueryTest("file_split_for_small_table",
    """
      |SELECT key, value FROM src SORT BY key, value
    """.stripMargin)
  //小写问题
  test("Spark-4041: lowercase issue") {
    TestHive.sql("CREATE TABLE tb (KEY INT, VALUE STRING) STORED AS ORC")
    TestHive.sql("insert into table tb select key, value from src")
    TestHive.sql("select KEY from tb where VALUE='just_for_test' limit 5").collect()
    TestHive.sql("drop table tb")
  }
  //时间戳查询空值
  test("Spark-4077: timestamp query for null value") {
    TestHive.sql("DROP TABLE IF EXISTS timestamp_query_null")
    TestHive.sql(
      """
        CREATE EXTERNAL TABLE timestamp_query_null (time TIMESTAMP,id INT)
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
      """.stripMargin)
    val location =
      Utils.getSparkClassLoader.getResource("data/files/issue-4077-data.txt").getFile()

    TestHive.sql(s"LOAD DATA LOCAL INPATH '$location' INTO TABLE timestamp_query_null")
    assert(TestHive.sql("SELECT time from timestamp_query_null limit 2").collect()
      === Array(Row(java.sql.Timestamp.valueOf("2014-12-11 00:00:00")), Row(null)))
    TestHive.sql("DROP TABLE timestamp_query_null")
  }
  //当使用从投影中选择查询时,属性是区分大小写的
  test("Spark-4959 Attributes are case sensitive when using a select query from a projection") {
    sql("create table spark_4959 (col1 string)")
    sql("""insert into table spark_4959 select "hi" from src limit 1""")
    table("spark_4959").select(
      'col1.as("CaseSensitiveColName"),
      'col1.as("CaseSensitiveColName2")).registerTempTable("spark_4959_2")

    assert(sql("select CaseSensitiveColName from spark_4959_2").head() === Row("hi"))
    assert(sql("select casesensitivecolname from spark_4959_2").head() === Row("hi"))
  }
}
