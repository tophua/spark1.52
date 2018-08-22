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

package org.apache.spark.sql.catalyst.analysis

/**
 * Represents the result of `Expression.checkInputDataTypes`.
  * 表示`Expression.checkInputDataTypes`的结果
 * We will throw `AnalysisException` in `CheckAnalysis` if `isFailure` is true.
  * 如果`isFailure`为真，我们将在`CheckAnalysis`中抛出`AnalysisException`
 */
trait TypeCheckResult {
  def isFailure: Boolean = !isSuccess
  def isSuccess: Boolean
}

object TypeCheckResult {

  /**
   * Represents the successful result of `Expression.checkInputDataTypes`.
    * 表示`Expression.checkInputDataTypes`的成功结果
   */
  object TypeCheckSuccess extends TypeCheckResult {
    def isSuccess: Boolean = true
  }

  /**
   * Represents the failing result of `Expression.checkInputDataTypes`,
   * with a error message to show the reason of failure.
    * 表示`Expression.checkInputDataTypes`的失败结果,并显示错误消息以显示失败原因
   */
  case class TypeCheckFailure(message: String) extends TypeCheckResult {
    def isSuccess: Boolean = false
  }
}
