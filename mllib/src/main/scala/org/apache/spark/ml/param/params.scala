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

package org.apache.spark.ml.param

import java.lang.reflect.Modifier
import java.util.NoSuchElementException

import scala.annotation.varargs
import scala.collection.mutable
import scala.collection.JavaConverters._

import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.ml.util.Identifiable

/**
 * :: DeveloperApi ::
 * A param with self-contained documentation and optionally default value. Primitive-typed param
 * should use the specialized versions, which are more friendly to Java users.
 * 一个包含自包含文档和可选默认值的参数,原始类型的param应该使用专用版本,这些版本对Java用户更友好
 * @param parent parent object
 * @param name param name
 * @param doc documentation
 * @param isValid optional validation method which indicates if a value is valid.
 *                See [[ParamValidators]] for factory methods for common validation functions.
 * @tparam T param value type
 */
@DeveloperApi
class Param[T](val parent: String, val name: String, val doc: String, val isValid: T => Boolean)
  extends Serializable {

  def this(parent: Identifiable, name: String, doc: String, isValid: T => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: String, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue[T])

  def this(parent: Identifiable, name: String, doc: String) = this(parent.uid, name, doc)

  /**
   * Assert that the given value is valid for this parameter.
    * 断言给定值对此参数有效
   *
   * Note: Parameter checks involving interactions between multiple parameters should be
   *       implemented in [[Params.validateParams()]].  Checks for input/output columns should be
   *       implemented in [[org.apache.spark.ml.PipelineStage.transformSchema()]].
   *
   * DEVELOPERS: This method is only called by [[ParamPair]], which means that all parameters
   *             should be specified via [[ParamPair]].
   *
   * @throws IllegalArgumentException if the value is invalid
   */
  private[param] def validate(value: T): Unit = {
    if (!isValid(value)) {
      throw new IllegalArgumentException(s"$parent parameter $name given invalid value $value.")
    }
  }

  /** 
   *  Creates a param pair with the given value (for Java). 
   *  创建一个具有指定值的参数对(for Java)
   *  */
  def w(value: T): ParamPair[T] = this -> value

  /** 
   *  Creates a param pair with the given value (for Scala).
   *  创建一个具有指定值的参数对(for Scala)
   *   */
  def ->(value: T): ParamPair[T] = ParamPair(this, value)

  override final def toString: String = s"${parent}__$name"

  override final def hashCode: Int = toString.##

  override final def equals(obj: Any): Boolean = {
    obj match {
      case p: Param[_] => (p.parent == parent) && (p.name == name)
      case _ => false
    }
  }
}

/**
 * :: DeveloperApi ::
 * Factory methods for common validation functions for [[Param.isValid]].
 * The numerical methods only support Int, Long, Float, and Double.
 * 数值方法只支持Int, Long, Float, and Double
 */
@DeveloperApi
object ParamValidators {

  /** 
   *  (private[param]) Default validation always return true
   *  默认验证总是返回true 
   *  */
  private[param] def alwaysTrue[T]: T => Boolean = (_: T) => true

  /**
   * Private method for checking numerical types and converting to Double.
   * 提供方法用于检查数值类型转换为Double的方法
   * This is mainly for the sake of compilation; type checks are really handled
   * by [[Params]] setters and the [[ParamPair]] constructor.
   */
  private def getDouble[T](value: T): Double = value match {
    case x: Int => x.toDouble
    case x: Long => x.toDouble
    case x: Float => x.toDouble
    case x: Double => x.toDouble
    case _ =>
      // The type should be checked before this is ever called.
      //该类型应检查之前,这是所谓的,
      throw new IllegalArgumentException("Numerical Param validation failed because" +
        s" of unexpected input type: ${value.getClass}")
  }

  /** 
   *  Check if value > lowerBound 
   *  检查值>下界
   *  */
  def gt[T](lowerBound: Double): T => Boolean = { (value: T) =>
    getDouble(value) > lowerBound
  }

  /** 
   *  Check if value >= lowerBound
   *  检查值> =下界 
   *  */
  def gtEq[T](lowerBound: Double): T => Boolean = { (value: T) =>
    getDouble(value) >= lowerBound
  }

  /** 
   *  Check if value < upperBound 
   *  检查值＜上界
   *  */
  def lt[T](upperBound: Double): T => Boolean = { (value: T) =>
    getDouble(value) < upperBound
  }

  /** 
   *  Check if value <= upperBound 
   *  检查值<=上限
   *  */
  def ltEq[T](upperBound: Double): T => Boolean = { (value: T) =>
    getDouble(value) <= upperBound
  }

  /**
   * Check for value in range lowerBound to upperBound.
   * 检查值范围为上界下界
   * @param lowerInclusive  If true, check for value >= lowerBound.
   *                        If false, check for value > lowerBound.
   * @param upperInclusive  If true, check for value <= upperBound.
   *                        If false, check for value < upperBound.
   */
  def inRange[T](
      lowerBound: Double,
      upperBound: Double,
      lowerInclusive: Boolean,
      upperInclusive: Boolean): T => Boolean = { (value: T) =>
    val x: Double = getDouble(value)
    val lowerValid = if (lowerInclusive) x >= lowerBound else x > lowerBound
    val upperValid = if (upperInclusive) x <= upperBound else x < upperBound
    lowerValid && upperValid
  }

  /** Version of [[inRange()]] which uses inclusive be default: [lowerBound, upperBound] */
  def inRange[T](lowerBound: Double, upperBound: Double): T => Boolean = {
    inRange[T](lowerBound, upperBound, lowerInclusive = true, upperInclusive = true)
  }

  /** 
   *  Check for value in an allowed set of values.
   *  在允许的值集合中检查值 
   *  */
  def inArray[T](allowed: Array[T]): T => Boolean = { (value: T) =>
    allowed.contains(value)
  }

  /** 
   *  Check for value in an allowed set of values. 
   *  在允许的值集合中检查值
   *  */
  def inArray[T](allowed: java.util.List[T]): T => Boolean = { (value: T) =>
    allowed.contains(value)
  }

  /** 
   *  Check that the array length is greater than lowerBound. 
   *  检查数组的长度大于下界
   *  */
  def arrayLengthGt[T](lowerBound: Double): Array[T] => Boolean = { (value: Array[T]) =>
    value.length > lowerBound
  }
}

// specialize primitive-typed params because Java doesn't recognize scala.Double, scala.Int, ...

/**
 * :: DeveloperApi ::
 * Specialized version of [[Param[Double]]] for Java.
 */
@DeveloperApi
class DoubleParam(parent: String, name: String, doc: String, isValid: Double => Boolean)
  extends Param[Double](parent, name, doc, isValid) {

  def this(parent: String, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  def this(parent: Identifiable, name: String, doc: String, isValid: Double => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) = this(parent.uid, name, doc)

  /** Creates a param pair with the given value (for Java). 创建具有给定值的param对（对于Java）*/
  override def w(value: Double): ParamPair[Double] = super.w(value)
}

/**
 * :: DeveloperApi ::
 * Specialized version of [[Param[Int]]] for Java.
 */
@DeveloperApi
class IntParam(parent: String, name: String, doc: String, isValid: Int => Boolean)
  extends Param[Int](parent, name, doc, isValid) {

  def this(parent: String, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  def this(parent: Identifiable, name: String, doc: String, isValid: Int => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) = this(parent.uid, name, doc)

  /** Creates a param pair with the given value (for Java).
    * 创建具有给定值的param对（对于Java）*/
  override def w(value: Int): ParamPair[Int] = super.w(value)
}

/**
 * :: DeveloperApi ::
 * Specialized version of [[Param[Float]]] for Java.
 */
@DeveloperApi
class FloatParam(parent: String, name: String, doc: String, isValid: Float => Boolean)
  extends Param[Float](parent, name, doc, isValid) {

  def this(parent: String, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  def this(parent: Identifiable, name: String, doc: String, isValid: Float => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) = this(parent.uid, name, doc)

  /** Creates a param pair with the given value (for Java). 创建具有给定值的param对（对于Java）*/
  override def w(value: Float): ParamPair[Float] = super.w(value)
}

/**
 * :: DeveloperApi ::
 * Specialized version of [[Param[Long]]] for Java.
 */
@DeveloperApi
class LongParam(parent: String, name: String, doc: String, isValid: Long => Boolean)
  extends Param[Long](parent, name, doc, isValid) {

  def this(parent: String, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  def this(parent: Identifiable, name: String, doc: String, isValid: Long => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) = this(parent.uid, name, doc)

  /** Creates a param pair with the given value (for Java). */
  override def w(value: Long): ParamPair[Long] = super.w(value)
}

/**
 * :: DeveloperApi ::
 * Specialized version of [[Param[Boolean]]] for Java.
 */
@DeveloperApi
class BooleanParam(parent: String, name: String, doc: String) // No need for isValid
  extends Param[Boolean](parent, name, doc) {

  def this(parent: Identifiable, name: String, doc: String) = this(parent.uid, name, doc)

  /** Creates a param pair with the given value (for Java). */
  override def w(value: Boolean): ParamPair[Boolean] = super.w(value)
}

/**
 * :: DeveloperApi ::
 * Specialized version of [[Param[Array[String]]]] for Java.
 */
@DeveloperApi
class StringArrayParam(parent: Params, name: String, doc: String, isValid: Array[String] => Boolean)
  extends Param[Array[String]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  /** Creates a param pair with a [[java.util.List]] of values (for Java and Python). */
  def w(value: java.util.List[String]): ParamPair[Array[String]] = w(value.asScala.toArray)
}

/**
 * :: DeveloperApi ::
 * Specialized version of [[Param[Array[Double]]]] for Java.
 */
@DeveloperApi
class DoubleArrayParam(parent: Params, name: String, doc: String, isValid: Array[Double] => Boolean)
  extends Param[Array[Double]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  /** Creates a param pair with a [[java.util.List]] of values (for Java and Python). */
  def w(value: java.util.List[java.lang.Double]): ParamPair[Array[Double]] =
    w(value.asScala.map(_.asInstanceOf[Double]).toArray)
}

/**
 * :: DeveloperApi ::
 * Specialized version of [[Param[Array[Int]]]] for Java.
 */
@DeveloperApi
class IntArrayParam(parent: Params, name: String, doc: String, isValid: Array[Int] => Boolean)
  extends Param[Array[Int]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  /** Creates a param pair with a [[java.util.List]] of values (for Java and Python). */
  def w(value: java.util.List[java.lang.Integer]): ParamPair[Array[Int]] =
    w(value.asScala.map(_.asInstanceOf[Int]).toArray)
}

/**
 * :: Experimental ::
 * A param and its value.
 */
@Experimental
case class ParamPair[T](param: Param[T], value: T) {
  // This is *the* place Param.validate is called.  Whenever a parameter is specified, we should
  // always construct a ParamPair so that validate is called.
  param.validate(value)
}

/**
 * :: DeveloperApi ::
 * Trait for components that take parameters. This also provides an internal param map to store
 * parameter values attached to the instance.
  * 具有参数的组件的特征,这还提供了一个内部参数映射,用于存储附加到实例的参数值
 */
@DeveloperApi
trait Params extends Identifiable with Serializable {

  /**
   * Returns all params sorted by their names. The default implementation uses Java reflection to
   * list all public methods that have no arguments and return [[Param]].
    *
    * 返回按名称排序的所有参数,默认实现使用Java反射列出所有没有参数的公共方法并返回[[Param]]
   *
   * Note: Developer should not use this method in constructor because we cannot guarantee that
   * this variable gets initialized before other params.
   */
  lazy val params: Array[Param[_]] = {
    val methods = this.getClass.getMethods
    methods.filter { m =>
        Modifier.isPublic(m.getModifiers) &&
          classOf[Param[_]].isAssignableFrom(m.getReturnType) &&
          m.getParameterTypes.isEmpty
      }.sortBy(_.getName)
      .map(m => m.invoke(this).asInstanceOf[Param[_]])
  }

  /**
   * Validates parameter values stored internally.
    * 验证内部存储的参数值
   * Raise an exception if any parameter value is invalid.
    * 如果任何参数值无效,则引发异常
   *
   * This only needs to check for interactions between parameters.
    * 这只需要检查参数之间的交互
   * Parameter value checks which do not depend on other parameters are handled by
   * [[Param.validate()]].  This method does not handle input/output column parameters;
   * those are checked during schema validation.
   */
  def validateParams(): Unit = {
    // Do nothing by default.  Override to handle Param interactions.
    //默认情况下不做任何事 覆盖以处理Param交互
  }

  /**
   * Explains a param.解释一个参数
   * @param param input param, must belong to this instance.
   * @return a string that contains the input param name, doc, and optionally its default value and
   *         the user-supplied value
   */
  def explainParam(param: Param[_]): String = {
    shouldOwn(param)
    val valueStr = if (isDefined(param)) {
      val defaultValueStr = getDefault(param).map("default: " + _)
      val currentValueStr = get(param).map("current: " + _)
      (defaultValueStr ++ currentValueStr).mkString("(", ", ", ")")
    } else {
      "(undefined)"
    }
    s"${param.name}: ${param.doc} $valueStr"
  }

  /**
   * Explains all params of this instance.
    * 解释这个实例的所有参数
   * @see [[explainParam()]]
   */
  def explainParams(): String = {
    params.map(explainParam).mkString("\n")
  }

  /** Checks whether a param is explicitly set.
    * 检查是否明确设置了参数 */
  final def isSet(param: Param[_]): Boolean = {
    shouldOwn(param)
    paramMap.contains(param)
  }

  /** Checks whether a param is explicitly set or has a default value.
    * 检查param是显式设置还是具有默认值*/
  final def isDefined(param: Param[_]): Boolean = {
    shouldOwn(param)
    defaultParamMap.contains(param) || paramMap.contains(param)
  }

  /** Tests whether this instance contains a param with a given name.
    * 测试此实例是否包含具有给定名称的参数*/
  def hasParam(paramName: String): Boolean = {
    params.exists(_.name == paramName)
  }

  /** Gets a param by its name. 按名称获取一个参数*/
  def getParam(paramName: String): Param[Any] = {
    params.find(_.name == paramName).getOrElse {
      throw new NoSuchElementException(s"Param $paramName does not exist.")
    }.asInstanceOf[Param[Any]]
  }

  /**
   * Sets a parameter in the embedded param map.
    * 在嵌入的参数映射中设置参数
   */
  protected final def set[T](param: Param[T], value: T): this.type = {
    set(param -> value)
  }

  /**
   * Sets a parameter (by name) in the embedded param map.
    * 在嵌入的参数映射中设置参数(按名称)
   */
  protected final def set(param: String, value: Any): this.type = {
    set(getParam(param), value)
  }

  /**
   * Sets a parameter in the embedded param map.
    * 在嵌入的参数映射中设置参数
   */
  protected final def set(paramPair: ParamPair[_]): this.type = {
    shouldOwn(paramPair.param)
    paramMap.put(paramPair)
    this
  }

  /**
   * Optionally returns the user-supplied value of a param.
    * (可选)返回用户提供的参数值
   */
  final def get[T](param: Param[T]): Option[T] = {
    shouldOwn(param)
    paramMap.get(param)
  }

  /**
   * Clears the user-supplied value for the input param.
    * 清除输入参数的用户提供的值
   */
  protected final def clear(param: Param[_]): this.type = {
    shouldOwn(param)
    paramMap.remove(param)
    this
  }

  /**
   * Gets the value of a param in the embedded param map or its default value. Throws an exception
   * if neither is set.
    * 获取嵌入的参数映射中的参数值或其默认值,如果两者都未设置,则抛出异常
   */
  final def getOrDefault[T](param: Param[T]): T = {
    shouldOwn(param)
    get(param).orElse(getDefault(param)).get
  }

  /** An alias for [[getOrDefault()]]. */
  protected final def $[T](param: Param[T]): T = getOrDefault(param)

  /**
   * Sets a default value for a param.设置参数的默认值
   * @param param  param to set the default value. Make sure that this param is initialized before
   *               this method gets called.
    *               param设置默认值,确保在调用此方法之前初始化此参数
   * @param value  the default value
   */
  protected final def setDefault[T](param: Param[T], value: T): this.type = {
    defaultParamMap.put(param -> value)
    this
  }

  /**
   * Sets default values for a list of params.
   * 设置参数列表的默认值
   * Note: Java developers should use the single-parameter [[setDefault()]].
   *       Annotating this with varargs can cause compilation failures due to a Scala compiler bug.
   *       See SPARK-9268.
   *
   * @param paramPairs  a list of param pairs that specify params and their default values to set
   *                    respectively. Make sure that the params are initialized before this method
   *                    gets called.
   */
  protected final def setDefault(paramPairs: ParamPair[_]*): this.type = {
    paramPairs.foreach { p =>
      setDefault(p.param.asInstanceOf[Param[Any]], p.value)
    }
    this
  }

  /**
   * Gets the default value of a parameter.
    * 获取参数的默认值
   */
  final def getDefault[T](param: Param[T]): Option[T] = {
    shouldOwn(param)
    defaultParamMap.get(param)
  }

  /**
   * Tests whether the input param has a default value set.
    * 测试输入参数是否具有默认值集
   */
  final def hasDefault[T](param: Param[T]): Boolean = {
    shouldOwn(param)
    defaultParamMap.contains(param)
  }

  /**
   * Creates a copy of this instance with the same UID and some extra params.
   * Subclasses should implement this method and set the return type properly.
    *
    * 使用相同的UID和一些额外的参数创建此实例的副本,子类应该实现此方法并正确设置返回类型
   *
   * @see [[defaultCopy()]]
   */
  def copy(extra: ParamMap): Params

  /**
   * Default implementation of copy with extra params.
    * 带有额外参数的副本的默认实现
   * It tries to create a new instance with the same UID.
    * 它尝试使用相同的UID创建新实例
   * Then it copies the embedded and extra parameters over and returns the new instance.
    * 然后它复制嵌入的和额外的参数并返回新实例
   */
  protected final def defaultCopy[T <: Params](extra: ParamMap): T = {
    val that = this.getClass.getConstructor(classOf[String]).newInstance(uid)
    copyValues(that, extra).asInstanceOf[T]
  }

  /**
   * Extracts the embedded default param values and user-supplied values, and then merges them with
   * extra values from input into a flat param map, where the latter value is used if there exist
   * conflicts, i.e., with ordering: default param values < user-supplied values < extra.
    * 提取嵌入的默认参数值和用户提供的值,然后将它们与输入中的额外值合并到一个平面参数映射中,
    * 如果存在冲突,则使用后一个值,即使用排序:默认参数值<用户提供 值<额外。
   */
  final def extractParamMap(extra: ParamMap): ParamMap = {
    defaultParamMap ++ paramMap ++ extra
  }

  /**
   * [[extractParamMap]] with no extra values.
   */
  final def extractParamMap(): ParamMap = {
    extractParamMap(ParamMap.empty)
  }

  /** Internal param map for user-supplied values.
    * 用户提供的值的内部参数映射*/
  private val paramMap: ParamMap = ParamMap.empty

  /** Internal param map for default values.
    * 内部参数映射表示默认值*/
  private val defaultParamMap: ParamMap = ParamMap.empty

  /** Validates that the input param belongs to this instance.
    * 验证输入参数是否属于此实例 */
  private def shouldOwn(param: Param[_]): Unit = {
    require(param.parent == uid && hasParam(param.name), s"Param $param does not belong to $this.")
  }

  /**
   * Copies param values from this instance to another instance for params shared by them.
    * 将此实例的参数值复制到另一个实例,用于它们共享的参数
   *
   * This handles default Params and explicitly set Params separately.
    * 这将处理默认的Params并单独显式设置Params
   * Default Params are copied from and to [[defaultParamMap]], and explicitly set Params are
   * copied from and to [[paramMap]].
   * Warning: This implicitly assumes that this [[Params]] instance and the target instance
   *          share the same set of default Params.
   *
   * @param to the target instance, which should work with the same set of default Params as this
   *           source instance
   * @param extra extra params to be copied to the target's [[paramMap]]
   * @return the target instance with param values copied
   */
  protected def copyValues[T <: Params](to: T, extra: ParamMap = ParamMap.empty): T = {
    val map = paramMap ++ extra
    params.foreach { param =>
      // copy default Params
      if (defaultParamMap.contains(param) && to.hasParam(param.name)) {
        to.defaultParamMap.put(to.getParam(param.name), defaultParamMap(param))
      }
      // copy explicitly set Params
      if (map.contains(param) && to.hasParam(param.name)) {
        to.set(param.name, map(param))
      }
    }
    to
  }
}

/**
 * :: DeveloperApi ::
 * Java-friendly wrapper for [[Params]].
 * Java developers who need to extend [[Params]] should use this class instead.
 * If you need to extend a abstract class which already extends [[Params]], then that abstract
 * class should be Java-friendly as well.
 */
@DeveloperApi
abstract class JavaParams extends Params

/**
 * :: Experimental ::
 * A param to value map.
 */
@Experimental
final class ParamMap private[ml] (private val map: mutable.Map[Param[Any], Any])
  extends Serializable {

  /* DEVELOPERS: About validating parameter values
   *   This and ParamPair are the only two collections of parameters.
   *   This class should always create ParamPairs when
   *   specifying new parameter values.  ParamPair will then call Param.validate().
   */

  /**
   * Creates an empty param map.
    * 创建一个空的参数映射
   */
  def this() = this(mutable.Map.empty)

  /**
   * Puts a (param, value) pair (overwrites if the input param exists).
    * 放入（param，value）对（如果输入param存在则覆盖）
   */
  def put[T](param: Param[T], value: T): this.type = put(param -> value)

  /**
   * Puts a list of param pairs (overwrites if the input params exists).
   */
  @varargs
  def put(paramPairs: ParamPair[_]*): this.type = {
    paramPairs.foreach { p =>
      map(p.param.asInstanceOf[Param[Any]]) = p.value
    }
    this
  }

  /**
   * Optionally returns the value associated with a param.
    * （可选）返回与param关联的值
   */
  def get[T](param: Param[T]): Option[T] = {
    map.get(param.asInstanceOf[Param[Any]]).asInstanceOf[Option[T]]
  }

  /**
   * Returns the value associated with a param or a default value.
    * 返回与param或默认值关联的值
   */
  def getOrElse[T](param: Param[T], default: T): T = {
    get(param).getOrElse(default)
  }

  /**
   * Gets the value of the input param or its default value if it does not exist.
    * 获取输入参数的值或其默认值（如果它不存在）
   * Raises a NoSuchElementException if there is no value associated with the input param.
   */
  def apply[T](param: Param[T]): T = {
    get(param).getOrElse {
      throw new NoSuchElementException(s"Cannot find param ${param.name}.")
    }
  }

  /**
   * Checks whether a parameter is explicitly specified.
    * 检查是否明确指定了参数
   */
  def contains(param: Param[_]): Boolean = {
    map.contains(param.asInstanceOf[Param[Any]])
  }

  /**
   * Removes a key from this map and returns its value associated previously as an option.
    * 从此映射中删除键并返回其先前关联的值作为选项
   */
  def remove[T](param: Param[T]): Option[T] = {
    map.remove(param.asInstanceOf[Param[Any]]).asInstanceOf[Option[T]]
  }

  /**
   * Filters this param map for the given parent.
    * 过滤此给定父级的参数映射
   */
  def filter(parent: Params): ParamMap = {
    val filtered = map.filterKeys(_.parent == parent)
    new ParamMap(filtered.asInstanceOf[mutable.Map[Param[Any], Any]])
  }

  /**
   * Creates a copy of this param map.
    * 创建此param映射的副本
   */
  def copy: ParamMap = new ParamMap(map.clone())

  override def toString: String = {
    map.toSeq.sortBy(_._1.name).map { case (param, value) =>
      s"\t${param.parent}-${param.name}: $value"
    }.mkString("{\n", ",\n", "\n}")
  }

  /**
   * Returns a new param map that contains parameters in this map and the given map,
   * where the latter overwrites this if there exist conflicts.
    * 返回一个新的param映射,其中包含此映射中的参数和给定的映射,如果存在冲突,后者将覆盖此映射。
   */
  def ++(other: ParamMap): ParamMap = {
    // TODO: Provide a better method name for Java users.
    new ParamMap(this.map ++ other.map)
  }

  /**
   * Adds all parameters from the input param map into this param map.
    * 将输入参数映射中的所有参数添加到此参数映射中
   */
  def ++=(other: ParamMap): this.type = {
    // TODO: Provide a better method name for Java users.
    this.map ++= other.map
    this
  }

  /**
   * Converts this param map to a sequence of param pairs.
    * 将此参数映射到一系列素数对
   */
  def toSeq: Seq[ParamPair[_]] = {
    map.toSeq.map { case (param, value) =>
      ParamPair(param, value)
    }
  }

  /**
   * Number of param pairs in this map.
   */
  def size: Int = map.size
}

@Experimental
object ParamMap {

  /**
   * Returns an empty param map.
    * 返回一个空的param映射
   */
  def empty: ParamMap = new ParamMap()

  /**
   * Constructs a param map by specifying its entries.
    * 通过指定其条目构造一个param映射
   */
  @varargs
  def apply(paramPairs: ParamPair[_]*): ParamMap = {
    new ParamMap().put(paramPairs: _*)
  }
}
