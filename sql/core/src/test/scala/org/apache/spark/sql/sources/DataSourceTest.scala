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

import org.apache.spark.sql._


private[sql] abstract class DataSourceTest extends QueryTest {
  protected def _sqlContext: SQLContext

  // We want to test some edge cases.
  //我们想测试一些边缘案件,大小写不敏感的Insensitive
  protected lazy val caseInsensitiveContext: SQLContext = {
    val ctx = new SQLContext(_sqlContext.sparkContext)
    ctx.setConf(SQLConf.CASE_SENSITIVE, false) //大小写敏感
    ctx
  }
  /**
   * expectedAnswer 预期的答案 
   */
  protected def sqlTest(sqlString: String, expectedAnswer: Seq[Row]) {
    test(sqlString) {
      checkAnswer(caseInsensitiveContext.sql(sqlString), expectedAnswer)
    }
  }

}
