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

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines all the filters that we can push down to the data sources.
//此文件定义了我们可以下推到数据源的所有过滤器
////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * A filter predicate for data sources.
  * 数据源的过滤谓词
 *
 * @since 1.3.0
 */
abstract class Filter

/**
 * A filter that evaluates to `true` iff the attribute evaluates to a value
 * equal to `value`.
  * 如果属性求值为等于“value”的值，则求值为“true”的过滤器
 *
 * @since 1.3.0
 */
case class EqualTo(attribute: String, value: Any) extends Filter

/**
 * Performs equality comparison, similar to [[EqualTo]]. However, this differs from [[EqualTo]]
 * in that it returns `true` (rather than NULL) if both inputs are NULL, and `false`
 * (rather than NULL) if one of the input is NULL and the other is not NULL.
  * 执行相等比较,类似于[[EqualTo]],但是,这与[[EqualTo]]的不同之处在于,如果两个输入都为NULL，
  * 则返回“true”（而不是NULL）;如果其中一个输入为NULL而另一个不为NULL,则返回“false”（而不是NULL）。
 *
 * @since 1.5.0
 */
case class EqualNullSafe(attribute: String, value: Any) extends Filter

/**
 * A filter that evaluates to `true` iff the attribute evaluates to a value
 * greater than `value`.
  * 如果属性求值大于“value”的值,则求值为“true”的过滤器
 *
 * @since 1.3.0
 */
case class GreaterThan(attribute: String, value: Any) extends Filter

/**
 * A filter that evaluates to `true` iff the attribute evaluates to a value
 * greater than or equal to `value`.
  * 如果属性求值大于或等于`value`的值,则求值为“true”的过滤器
 *
 * @since 1.3.0
 */
case class GreaterThanOrEqual(attribute: String, value: Any) extends Filter

/**
 * A filter that evaluates to `true` iff the attribute evaluates to a value
 * less than `value`.
  * 如果属性求值为小于“value”的值,则求值为“true”的过滤器
 *
 * @since 1.3.0
 */
case class LessThan(attribute: String, value: Any) extends Filter

/**
 * A filter that evaluates to `true` iff the attribute evaluates to a value
 * less than or equal to `value`.
  *
  * 如果属性求值小于或等于“value”的值,则求值为“true”的过滤器
 *
 * @since 1.3.0
 */
case class LessThanOrEqual(attribute: String, value: Any) extends Filter

/**
 * A filter that evaluates to `true` iff the attribute evaluates to one of the values in the array.
  * 如果属性求值为数组中的某个值,则求值为“true”的过滤器
 *
 * @since 1.3.0
 */
case class In(attribute: String, values: Array[Any]) extends Filter

/**
 * A filter that evaluates to `true` iff the attribute evaluates to null.
  * 如果属性求值为null,则求值为“true”的过滤器
 *
 * @since 1.3.0
 */
case class IsNull(attribute: String) extends Filter

/**
 * A filter that evaluates to `true` iff the attribute evaluates to a non-null value.
  * 如果属性求值为非空值,则求值为“true”的过滤器
 *
 * @since 1.3.0
 */
case class IsNotNull(attribute: String) extends Filter

/**
 * A filter that evaluates to `true` iff both `left` or `right` evaluate to `true`.
  * 如果“left”或“right”都评估为“true”,则计算结果为“true”的过滤器
 *
 * @since 1.3.0
 */
case class And(left: Filter, right: Filter) extends Filter

/**
 * A filter that evaluates to `true` iff at least one of `left` or `right` evaluates to `true`.
  * 如果`left`或`right`中的至少一个求值为“true”,则求值为“true”的过滤器
 *
 * @since 1.3.0
 */
case class Or(left: Filter, right: Filter) extends Filter

/**
 * A filter that evaluates to `true` iff `child` is evaluated to `false`.
  * 评估为“true”iff“child”的过滤器被评估为“false”
 *
 * @since 1.3.0
 */
case class Not(child: Filter) extends Filter

/**
 * A filter that evaluates to `true` iff the attribute evaluates to
 * a string that starts with `value`.
  *
  * 如果属性求值为以“value”开头的字符串,则求值为“true”的过滤器
 *
 * @since 1.3.1
 */
case class StringStartsWith(attribute: String, value: String) extends Filter

/**
 * A filter that evaluates to `true` iff the attribute evaluates to
 * a string that starts with `value`.
  * 如果属性求值为以“value”开头的字符串,则求值为“true”的过滤器
 *
 * @since 1.3.1
 */
case class StringEndsWith(attribute: String, value: String) extends Filter

/**
 * A filter that evaluates to `true` iff the attribute evaluates to
 * a string that contains the string `value`.
  *
  * 如果属性求值为包含字符串“value”的字符串,则求值为“true”的过滤器
 *
 * @since 1.3.1
 */
case class StringContains(attribute: String, value: String) extends Filter
