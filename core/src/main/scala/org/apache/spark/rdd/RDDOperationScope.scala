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

package org.apache.spark.rdd

import java.util.concurrent.atomic.AtomicInteger

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude, JsonPropertyOrder}
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.spark.{Logging, SparkContext}

/**
 * A general, named code block representing an operation that instantiates RDDs.
 * 指定代码块代表操作实例化RDDS
 * All RDDs instantiated in the corresponding code block will store a pointer to this object.
 * Examples include, but will not be limited to, existing RDD operations, such as textFile,
 * reduceByKey, and treeAggregate.
 *所有的RDDS实例化对应的代码块将存储一个指向该对象,例如包括,不限于现有RDD操作,如文本文件,
 * An operation scope may be nested in other scopes. For instance, a SQL query may enclose
 * scopes associated with the public RDD APIs it uses under the hood.
 * 操作范围可以嵌套在其他作用域中,例如,SQL查询可以用公共RDD API使用的相关范围
 * There is no particular relationship between an operation scope and a stage or a job.
 * A scope may live inside one stage (e.g. map) or span across multiple jobs (e.g. take).

  JsonInclude(Include.NON_NULL)
  属性为NULL 不序列化
  将该标记放在属性上，如果该属性为NULL则不参与序列化
  如果放在类上边,那对这个类的全部属性起作用
  Include.Include.ALWAYS 默认
  Include.NON_DEFAULT 属性为默认值不序列化
  Include.NON_EMPTY 属性为 空（“”） 或者为 NULL 都不序列化
  Include.NON_NULL 属性为NULL 不序列化
 */
@JsonInclude(Include.NON_NULL)
@JsonPropertyOrder(Array("id", "name", "parent")) //属性排序
private[spark] class RDDOperationScope(
    val name: String,
    val parent: Option[RDDOperationScope] = None,
    val id: String = RDDOperationScope.nextScopeId().toString) {

  def toJson: String = {
    //将对象转换成json字符串
    RDDOperationScope.jsonMapper.writeValueAsString(this)
  }

  /**
   * Return a list of scopes that this scope is a part of, including this scope itself.
   * The result is ordered from the outermost scope (eldest ancestor) to this scope.
    * 返回此范围是其范围的范围的列表,包括此范围本身,结果是从最外层的范围（最老的祖先）到这个范围,
   */
  @JsonIgnore
  def getAllScopes: Seq[RDDOperationScope] = {
    parent.map(_.getAllScopes).getOrElse(Seq.empty) ++ Seq(this)
  }

  override def equals(other: Any): Boolean = {
    other match {
      case s: RDDOperationScope =>
        id == s.id && name == s.name && parent == s.parent
      case _ => false
    }
  }

  override def toString: String = toJson
}

/**
 * A collection of utility methods to construct a hierarchical representation of RDD scopes.
 * An RDD scope tracks the series of operations that created a given RDD.
  * 用于构建RDD范围的分层表示的实用方法的集合,RDD范围跟踪创建给定RDD的一系列操作,
 */
private[spark] object RDDOperationScope extends Logging {
  private val jsonMapper = new ObjectMapper().registerModule(DefaultScalaModule)
  private val scopeCounter = new AtomicInteger(0)
  //readValue将json字符串转换成java对象
  def fromJson(s: String): RDDOperationScope = {
    jsonMapper.readValue(s, classOf[RDDOperationScope])
  }

  /** 
   *  Return a globally unique operation scope ID.
   *  返回一个全局唯一ID的操作范围 
   *  */
  def nextScopeId(): Int = scopeCounter.getAndIncrement

  /**
   * Execute the given body such that all RDDs created in this body will have the same scope.
   * The name of the scope will be the first method name in the stack trace that is not the
   * same as this method's.
    * 执行给定的body，使得在本体中创建的所有RDD将具有相同的范围,作用域的名称将是堆栈跟踪中与此方法不同的第一个方法名称。
    *
   * 柯里化函数
   * Note: Return statements are NOT allowed in body.
    * 注意：返回语句不允许在body。
   */
  private[spark] def withScope[T](
      sc: SparkContext,
      allowNesting: Boolean = false)(body: => T): T = {
    val ourMethodName = "withScope"
    //Thread.currentThread().getContextClassLoader,可以获取当前线程的引用,getContextClassLoader用来获取线程的上下文类加载器
    //getStackTrace()返回一个表示该线程堆栈转储的堆栈跟踪元素数组
    val callerMethodName = Thread.currentThread.getStackTrace()
      .dropWhile(_.getMethodName != ourMethodName)//从左向右丢弃元素,直到条件p不成立
      .find(_.getMethodName != ourMethodName)
      .map(_.getMethodName)
      .getOrElse {
        // Log a warning just in case, but this should almost certainly never happen
        logWarning("No valid method name for this RDD operation scope!")
        "N/A"
      }
    //调用柯里化withScope函数(下面)
    withScope[T](sc, callerMethodName, allowNesting, ignoreParent = false)(body)
  }

  /**
   * Execute the given body such that all RDDs created in this body will have the same scope.
   * 执行给定自身RDD的代码主休,创建自身相同的范围,如果允许嵌套
   * If nesting is allowed, any subsequent calls to this method in the given body will instantiate
   * child scopes that are nested within our scope. Otherwise, these calls will take no effect.
    *
   * 如果允许嵌套,则在给定正文中对此方法的任何后续调用将实例化嵌套在我们的范围内的子范围,否则,这些调用将不起作用。
    *
   * Additionally, the caller of this method may optionally ignore the configurations and scopes
   * set by the higher level caller. In this case, this method will ignore the parent caller's
   * intention to disallow nesting, and the new scope instantiated will not have a parent. This
   * is useful for scoping physical operations in Spark SQL, for instance.
    *
    * 此外，该方法的调用者可以可选地忽略由较高级别的呼叫者设置的配置和范围,在这种情况下,此方法将忽略父调用者禁止嵌套的意图，
    * 并且实例化的新范围将不具有父级,这对于例如Spark SQL中的物理操作来说很有用。
   *
   * Note: Return statements are NOT allowed in body.
   */
  private[spark] def withScope[T](
      sc: SparkContext,
      name: String,
      allowNesting: Boolean,
      ignoreParent: Boolean)(body: => T): T = {
    // Save the old scope to restore it later
    //保存旧范围以后恢复
    val scopeKey = SparkContext.RDD_SCOPE_KEY
    val noOverrideKey = SparkContext.RDD_SCOPE_NO_OVERRIDE_KEY
    val oldScopeJson = sc.getLocalProperty(scopeKey)
    val oldScope = Option(oldScopeJson).map(RDDOperationScope.fromJson)
    val oldNoOverride = sc.getLocalProperty(noOverrideKey)
    try {
      if (ignoreParent) {
        // Ignore all parent settings and scopes and start afresh with our own root scope
        //忽略所有父设置和范围，并使用您自己的根范围重新开始
        sc.setLocalProperty(scopeKey, new RDDOperationScope(name).toJson)
      } else if (sc.getLocalProperty(noOverrideKey) == null) {
        // Otherwise, set the scope only if the higher level caller allows us to do so
        //否则，仅当较高级别的调用者允许我们这样做时才设置范围
        sc.setLocalProperty(scopeKey, new RDDOperationScope(name, oldScope).toJson)
      }
      // Optionally disallow the child body to override our scope
      //可选择不允许子体覆盖我们的范围
      if (!allowNesting) {
        sc.setLocalProperty(noOverrideKey, "true")
      }
      body
    } finally {
      // Remember to restore any state that was modified before exiting
      //记住要恢复在退出之前被修改的任何状态
      sc.setLocalProperty(scopeKey, oldScopeJson)
      sc.setLocalProperty(noOverrideKey, oldNoOverride)
    }
  }
}
