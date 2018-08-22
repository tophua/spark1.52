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

import javax.annotation.Nullable

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types._


/**
 * A collection of [[Rule Rules]] that can be used to coerce differing types that
 * participate in operations into compatible ones.  Most of these rules are based on Hive semantics,
 * but they do not introduce any dependencies on the hive codebase.  For this reason they remain in
 * Catalyst until we have a more standard set of coercions.
  * [规则规则]的集合,可用于将参与操作的不同类型强制转换为兼容的类型,
  * 这些规则中的大多数都基于Hive语义,但它们不会对hive代码库引入任何依赖性,
  * 出于这个原因,他们会留在Catalyst中,直到我们有更多标准的强制措施。
 */
object HiveTypeCoercion {

  val typeCoercionRules =
    PropagateTypes ::
      InConversion ::
      WidenSetOperationTypes ::
      PromoteStrings ::
      DecimalPrecision ::
      BooleanEquality ::
      StringToIntegralCasts ::
      FunctionArgumentConversion ::
      CaseWhenCoercion ::
      IfCoercion ::
      Division ::
      PropagateTypes ::
      ImplicitTypeCasts ::
      DateTimeOperations ::
      Nil

  // See https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types.
  // The conversion for integral and floating point types have a linear widening hierarchy:
  //积分和浮点类型的转换具有线性扩展层次结构
  private val numericPrecedence =
    IndexedSeq(
      ByteType,
      ShortType,
      IntegerType,
      LongType,
      FloatType,
      DoubleType)

  /**
   * Find the tightest common type of two types that might be used in a binary expression.
    * 找到可能在二进制表达式中使用的两种类型的最紧密的常见类型
   * This handles all numeric types except fixed-precision decimals interacting with each other or
   * with primitive types, because in that case the precision and scale of the result depends on
   * the operation. Those rules are implemented in [[HiveTypeCoercion.DecimalPrecision]].
    * 这将处理除固定精度小数彼此交互或与基本类型交互的所有数字类型,因为在这种情况下,
    * 结果的精度和比例取决于操作,这些规则在[[HiveTypeCoercion.DecimalPrecision]]中实现
   */
  val findTightestCommonTypeOfTwo: (DataType, DataType) => Option[DataType] = {
    case (t1, t2) if t1 == t2 => Some(t1)
    case (NullType, t1) => Some(t1)
    case (t1, NullType) => Some(t1)

    case (t1: IntegralType, t2: DecimalType) if t2.isWiderThan(t1) =>
      Some(t2)
    case (t1: DecimalType, t2: IntegralType) if t1.isWiderThan(t2) =>
      Some(t1)

    // Promote numeric types to the highest of the two
      //将数字类型提升到两者中的最高者
    case (t1, t2) if Seq(t1, t2).forall(numericPrecedence.contains) =>
      val index = numericPrecedence.lastIndexWhere(t => t == t1 || t == t2)
      Some(numericPrecedence(index))

    case _ => None
  }

  /** Similar to [[findTightestCommonType]], but can promote all the way to StringType.
    * 与[[findTightestCommonType]]类似,但可以一直提升为StringType*/
  private def findTightestCommonTypeToString(left: DataType, right: DataType): Option[DataType] = {
    findTightestCommonTypeOfTwo(left, right).orElse((left, right) match {
      case (StringType, t2: AtomicType) if t2 != BinaryType && t2 != BooleanType => Some(StringType)
      case (t1: AtomicType, StringType) if t1 != BinaryType && t1 != BooleanType => Some(StringType)
      case _ => None
    })
  }

  /**
   * Similar to [[findTightestCommonType]], if can not find the TightestCommonType, try to use
   * [[findTightestCommonTypeToString]] to find the TightestCommonType.
    * 与[[findTightestCommonType]]类似,如果找不到TightestCommonType,
    * 请尝试使用[[findTightestCommonTypeToString]]查找TightestCommonType
   */
  private def findTightestCommonTypeAndPromoteToString(types: Seq[DataType]): Option[DataType] = {
    types.foldLeft[Option[DataType]](Some(NullType))((r, c) => r match {
      case None => None
      case Some(d) =>
        findTightestCommonTypeToString(d, c)
    })
  }

  /**
   * Find the tightest common type of a set of types by continuously applying
   * `findTightestCommonTypeOfTwo` on these types.
    * 通过在这些类型上不断应用`findTightestCommonTypeOfTwo`,找到一组类型中最紧凑的常见类型。
   */
  private def findTightestCommonType(types: Seq[DataType]): Option[DataType] = {
    types.foldLeft[Option[DataType]](Some(NullType))((r, c) => r match {
      case None => None
      case Some(d) => findTightestCommonTypeOfTwo(d, c)
    })
  }

  private def findWiderTypeForTwo(t1: DataType, t2: DataType): Option[DataType] = (t1, t2) match {
    case (t1: DecimalType, t2: DecimalType) =>
      Some(DecimalPrecision.widerDecimalType(t1, t2))
    case (t: IntegralType, d: DecimalType) =>
      Some(DecimalPrecision.widerDecimalType(DecimalType.forType(t), d))
    case (d: DecimalType, t: IntegralType) =>
      Some(DecimalPrecision.widerDecimalType(DecimalType.forType(t), d))
    case (t: FractionalType, d: DecimalType) =>
      Some(DoubleType)
    case (d: DecimalType, t: FractionalType) =>
      Some(DoubleType)
    case _ =>
      findTightestCommonTypeToString(t1, t2)
  }

  private def findWiderCommonType(types: Seq[DataType]) = {
    types.foldLeft[Option[DataType]](Some(NullType))((r, c) => r match {
      case Some(d) => findWiderTypeForTwo(d, c)
      case None => None
    })
  }

  /**
   * Applies any changes to [[AttributeReference]] data types that are made by other rules to
   * instances higher in the query tree.
    * 将其他规则对[[AttributeReference]]数据类型的任何更改应用于查询树中较高的实例
   */
  object PropagateTypes extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {

      // No propagation required for leaf nodes.
      //叶节点不需要传播
      case q: LogicalPlan if q.children.isEmpty => q

      // Don't propagate types from unresolved children.
        //不要从未解决的孩子传播类型
      case q: LogicalPlan if !q.childrenResolved => q

      case q: LogicalPlan =>
        val inputMap = q.inputSet.toSeq.map(a => (a.exprId, a)).toMap
        q transformExpressions {
          case a: AttributeReference =>
            inputMap.get(a.exprId) match {
              // This can happen when a Attribute reference is born in a non-leaf node, for example
              // due to a call to an external script like in the Transform operator.
              // TODO: Perhaps those should actually be aliases?
              case None => a
              // Leave the same if the dataTypes match.
              case Some(newType) if a.dataType == newType.dataType => a
              case Some(newType) =>
                logDebug(s"Promoting $a to $newType in ${q.simpleString}")
                newType
            }
        }
    }
  }

  /**
   * Widens numeric types and converts strings to numbers when appropriate.
    * 扩展数字类型并在适当时将字符串转换为数字
   *
   * Loosely based on rules from "Hadoop: The Definitive Guide" 2nd edition, by Tom White
   *松散地基于汤姆怀特的“Hadoop：The Definitive Guide”第2版的规则
    *
   * The implicit conversion rules can be summarized as follows:
    * 隐式转换规则可归纳如下：
   *   - Any integral numeric type can be implicitly converted to a wider type.
    *   任何整数数字类型都可以隐式转换为更宽的类型
   *   - All the integral numeric types, FLOAT, and (perhaps surprisingly) STRING can be implicitly
   *     converted to DOUBLE.
    *     所有整数数字类型FLOAT和(可能令人惊讶的)STRING都可以隐式转换为DOUBLE。
   *   - TINYINT, SMALLINT, and INT can all be converted to FLOAT.
    *   TINYINT，SMALLINT和INT都可以转换为FLOAT。
   *   - BOOLEAN types cannot be converted to any other type.
    *   BOOLEAN类型无法转换为任何其他类型
   *   - Any integral numeric type can be implicitly converted to decimal type.
    *   任何整数数字类型都可以隐式转换为十进制类型
   *   - two different decimal types will be converted into a wider decimal type for both of them.
    *   两种不同的十进制类型将被转换为更宽的十进制类型
   *   - decimal type will be converted into double if there float or double together with it.
    *   如果有浮点数或双精度数,则十进制类型将转换为double
   *
   * Additionally, all types when UNION-ed with strings will be promoted to strings.
    * 此外,使用字符串UNION编辑时的所有类型都将提升为字符串
   * Other string conversions are handled by PromoteStrings.
    * 其他字符串转换由PromoteStrings处理
   *
   * Widening types might result in loss of precision in the following cases:
    * 在以下情况下,加宽类型可能会导致精度损失
   * - IntegerType to FloatType
   * - LongType to FloatType
   * - LongType to DoubleType
   * - DecimalType to Double
   *
   * This rule is only applied to Union/Except/Intersect
   */
  object WidenSetOperationTypes extends Rule[LogicalPlan] {

    private[this] def widenOutputTypes(
        planName: String,
        left: LogicalPlan,
        right: LogicalPlan): (LogicalPlan, LogicalPlan) = {
      require(left.output.length == right.output.length)

      val castedTypes = left.output.zip(right.output).map {
        case (lhs, rhs) if lhs.dataType != rhs.dataType =>
          findWiderTypeForTwo(lhs.dataType, rhs.dataType)
        case other => None
      }

      def castOutput(plan: LogicalPlan): LogicalPlan = {
        val casted = plan.output.zip(castedTypes).map {
          case (e, Some(dt)) if e.dataType != dt =>
            Alias(Cast(e, dt), e.name)()
          case (e, _) => e
        }
        Project(casted, plan)
      }

      if (castedTypes.exists(_.isDefined)) {
        (castOutput(left), castOutput(right))
      } else {
        (left, right)
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case p if p.analyzed => p

      case s @ SetOperation(left, right) if s.childrenResolved
          && left.output.length == right.output.length && !s.resolved =>
        val (newLeft, newRight) = widenOutputTypes(s.nodeName, left, right)
        s.makeCopy(Array(newLeft, newRight))
    }
  }

  /**
   * Promotes strings that appear in arithmetic expressions.
    * 提升出现在算术表达式中的字符串
   */
  object PromoteStrings extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
      // Skip nodes who's children have not been resolved yet.
      //跳过那些孩子尚未解决的节点
      case e if !e.childrenResolved => e

      case a @ BinaryArithmetic(left @ StringType(), right @ DecimalType.Expression(_, _)) =>
        a.makeCopy(Array(Cast(left, DecimalType.SYSTEM_DEFAULT), right))
      case a @ BinaryArithmetic(left @ DecimalType.Expression(_, _), right @ StringType()) =>
        a.makeCopy(Array(left, Cast(right, DecimalType.SYSTEM_DEFAULT)))

      case a @ BinaryArithmetic(left @ StringType(), right) =>
        a.makeCopy(Array(Cast(left, DoubleType), right))
      case a @ BinaryArithmetic(left, right @ StringType()) =>
        a.makeCopy(Array(left, Cast(right, DoubleType)))

      // For equality between string and timestamp we cast the string to a timestamp
      // so that things like rounding of subsecond precision does not affect the comparison.
        //对于字符串和时间戳之间的相等,我们将字符串转换为时间戳,以便像亚秒精度的舍入之类的事情不会影响比较
      case p @ Equality(left @ StringType(), right @ TimestampType()) =>
        p.makeCopy(Array(Cast(left, TimestampType), right))
      case p @ Equality(left @ TimestampType(), right @ StringType()) =>
        p.makeCopy(Array(left, Cast(right, TimestampType)))

      // We should cast all relative timestamp/date/string comparison into string comparisions
      // This behaves as a user would expect because timestamp strings sort lexicographically.
      // i.e. TimeStamp(2013-01-01 00:00 ...) < "2014" = true
      case p @ BinaryComparison(left @ StringType(), right @ DateType()) =>
        p.makeCopy(Array(left, Cast(right, StringType)))
      case p @ BinaryComparison(left @ DateType(), right @ StringType()) =>
        p.makeCopy(Array(Cast(left, StringType), right))
      case p @ BinaryComparison(left @ StringType(), right @ TimestampType()) =>
        p.makeCopy(Array(left, Cast(right, StringType)))
      case p @ BinaryComparison(left @ TimestampType(), right @ StringType()) =>
        p.makeCopy(Array(Cast(left, StringType), right))

      // Comparisons between dates and timestamps. 日期和时间戳之间的比较
      case p @ BinaryComparison(left @ TimestampType(), right @ DateType()) =>
        p.makeCopy(Array(Cast(left, StringType), Cast(right, StringType)))
      case p @ BinaryComparison(left @ DateType(), right @ TimestampType()) =>
        p.makeCopy(Array(Cast(left, StringType), Cast(right, StringType)))

      case p @ BinaryComparison(left @ StringType(), right) if right.dataType != StringType =>
        p.makeCopy(Array(Cast(left, DoubleType), right))
      case p @ BinaryComparison(left, right @ StringType()) if left.dataType != StringType =>
        p.makeCopy(Array(left, Cast(right, DoubleType)))

      case i @ In(a @ DateType(), b) if b.forall(_.dataType == StringType) =>
        i.makeCopy(Array(Cast(a, StringType), b))
      case i @ In(a @ TimestampType(), b) if b.forall(_.dataType == StringType) =>
        i.makeCopy(Array(a, b.map(Cast(_, TimestampType))))
      case i @ In(a @ DateType(), b) if b.forall(_.dataType == TimestampType) =>
        i.makeCopy(Array(Cast(a, StringType), b.map(Cast(_, StringType))))
      case i @ In(a @ TimestampType(), b) if b.forall(_.dataType == DateType) =>
        i.makeCopy(Array(Cast(a, StringType), b.map(Cast(_, StringType))))

      case Sum(e @ StringType()) => Sum(Cast(e, DoubleType))
      case SumDistinct(e @ StringType()) => Sum(Cast(e, DoubleType))
      case Average(e @ StringType()) => Average(Cast(e, DoubleType))
    }
  }

  /**
   * Convert all expressions in in() list to the left operator type
    * 将in()列表中的所有表达式转换为左侧运算符类型
   */
  object InConversion extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      case i @ In(a, b) if b.exists(_.dataType != a.dataType) =>
        i.makeCopy(Array(a, b.map(Cast(_, a.dataType))))
    }
  }

  // scalastyle:off
  /**
   * Calculates and propagates precision for fixed-precision decimals. Hive has a number of
   * rules for this based on the SQL standard and MS SQL:
    * 计算并传播固定精度小数的精度,Hive有许多基于SQL标准和MS SQL的规则：
   * https://cwiki.apache.org/confluence/download/attachments/27362075/Hive_Decimal_Precision_Scale_Support.pdf
   * https://msdn.microsoft.com/en-us/library/ms190476.aspx
   *
   * In particular, if we have expressions e1 and e2 with precision/scale p1/s2 and p2/s2
   * respectively, then the following operations have the following precision / scale:
   *
   *   Operation    Result Precision                        Result Scale
   *   ------------------------------------------------------------------------
   *   e1 + e2      max(s1, s2) + max(p1-s1, p2-s2) + 1     max(s1, s2)
   *   e1 - e2      max(s1, s2) + max(p1-s1, p2-s2) + 1     max(s1, s2)
   *   e1 * e2      p1 + p2 + 1                             s1 + s2
   *   e1 / e2      p1 - s1 + s2 + max(6, s1 + p2 + 1)      max(6, s1 + p2 + 1)
   *   e1 % e2      min(p1-s1, p2-s2) + max(s1, s2)         max(s1, s2)
   *   e1 union e2  max(s1, s2) + max(p1-s1, p2-s2)         max(s1, s2)
   *   sum(e1)      p1 + 10                                 s1
   *   avg(e1)      p1 + 4                                  s1 + 4
   *
   * Catalyst also has unlimited-precision decimals. For those, all ops return unlimited precision.
   *
   * To implement the rules for fixed-precision types, we introduce casts to turn them to unlimited
   * precision, do the math on unlimited-precision numbers, then introduce casts back to the
   * required fixed precision. This allows us to do all rounding and overflow handling in the
   * cast-to-fixed-precision operator.
   *
   * In addition, when mixing non-decimal types with decimals, we use the following rules:
   * - BYTE gets turned into DECIMAL(3, 0)
   * - SHORT gets turned into DECIMAL(5, 0)
   * - INT gets turned into DECIMAL(10, 0)
   * - LONG gets turned into DECIMAL(20, 0)
   * - FLOAT and DOUBLE cause fixed-length decimals to turn into DOUBLE
   *
   * Note: Union/Except/Interact is handled by WidenTypes
   */
  // scalastyle:on
  object DecimalPrecision extends Rule[LogicalPlan] {
    import scala.math.{max, min}

    private def isFloat(t: DataType): Boolean = t == FloatType || t == DoubleType

    // Returns the wider decimal type that's wider than both of them
    //返回比两者都宽的更宽的十进制类型
    def widerDecimalType(d1: DecimalType, d2: DecimalType): DecimalType = {
      widerDecimalType(d1.precision, d1.scale, d2.precision, d2.scale)
    }
    // max(s1, s2) + max(p1-s1, p2-s2), max(s1, s2)
    def widerDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): DecimalType = {
      val scale = max(s1, s2)
      val range = max(p1 - s1, p2 - s2)
      DecimalType.bounded(range + scale, scale)
    }

    private def promotePrecision(e: Expression, dataType: DataType): Expression = {
      PromotePrecision(Cast(e, dataType))
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {

      // fix decimal precision for expressions
      //修复表达式的小数精度
      case q => q.transformExpressions {
        // Skip nodes whose children have not been resolved yet
        //跳过其子级尚未解析的节点
        case e if !e.childrenResolved => e

        // Skip nodes who is already promoted
          //跳过已升级的节点
        case e: BinaryArithmetic if e.left.isInstanceOf[PromotePrecision] => e

        case Add(e1 @ DecimalType.Expression(p1, s1), e2 @ DecimalType.Expression(p2, s2)) =>
          val dt = DecimalType.bounded(max(s1, s2) + max(p1 - s1, p2 - s2) + 1, max(s1, s2))
          CheckOverflow(Add(promotePrecision(e1, dt), promotePrecision(e2, dt)), dt)

        case Subtract(e1 @ DecimalType.Expression(p1, s1), e2 @ DecimalType.Expression(p2, s2)) =>
          val dt = DecimalType.bounded(max(s1, s2) + max(p1 - s1, p2 - s2) + 1, max(s1, s2))
          CheckOverflow(Subtract(promotePrecision(e1, dt), promotePrecision(e2, dt)), dt)

        case Multiply(e1 @ DecimalType.Expression(p1, s1), e2 @ DecimalType.Expression(p2, s2)) =>
          val resultType = DecimalType.bounded(p1 + p2 + 1, s1 + s2)
          val widerType = widerDecimalType(p1, s1, p2, s2)
          CheckOverflow(Multiply(promotePrecision(e1, widerType), promotePrecision(e2, widerType)),
            resultType)

        case Divide(e1 @ DecimalType.Expression(p1, s1), e2 @ DecimalType.Expression(p2, s2)) =>
          var intDig = min(DecimalType.MAX_SCALE, p1 - s1 + s2)
          var decDig = min(DecimalType.MAX_SCALE, max(6, s1 + p2 + 1))
          val diff = (intDig + decDig) - DecimalType.MAX_SCALE
          if (diff > 0) {
            decDig -= diff / 2 + 1
            intDig = DecimalType.MAX_SCALE - decDig
          }
          val resultType = DecimalType.bounded(intDig + decDig, decDig)
          val widerType = widerDecimalType(p1, s1, p2, s2)
          CheckOverflow(Divide(promotePrecision(e1, widerType), promotePrecision(e2, widerType)),
            resultType)

        case Remainder(e1 @ DecimalType.Expression(p1, s1), e2 @ DecimalType.Expression(p2, s2)) =>
          val resultType = DecimalType.bounded(min(p1 - s1, p2 - s2) + max(s1, s2), max(s1, s2))
          // resultType may have lower precision, so we cast them into wider type first.
          //resultType可能具有较低的精度,因此我们首先将它们转换为更宽的类型。
          val widerType = widerDecimalType(p1, s1, p2, s2)
          CheckOverflow(Remainder(promotePrecision(e1, widerType), promotePrecision(e2, widerType)),
            resultType)

        case Pmod(e1 @ DecimalType.Expression(p1, s1), e2 @ DecimalType.Expression(p2, s2)) =>
          val resultType = DecimalType.bounded(min(p1 - s1, p2 - s2) + max(s1, s2), max(s1, s2))
          // resultType may have lower precision, so we cast them into wider type first.
          //resultType可能具有较低的精度,因此我们首先将它们转换为更宽的类型
          val widerType = widerDecimalType(p1, s1, p2, s2)
          CheckOverflow(Pmod(promotePrecision(e1, widerType), promotePrecision(e2, widerType)),
            resultType)

        case b @ BinaryComparison(e1 @ DecimalType.Expression(p1, s1),
                                  e2 @ DecimalType.Expression(p2, s2)) if p1 != p2 || s1 != s2 =>
          val resultType = widerDecimalType(p1, s1, p2, s2)
          b.makeCopy(Array(Cast(e1, resultType), Cast(e2, resultType)))

        // Promote integers inside a binary expression with fixed-precision decimals to decimals,
          //将二进制表达式中的整数提升为十进制的固定精度小数
        // and fixed-precision decimals in an expression with floats / doubles to doubles
          //表达式中的固定精度小数和浮点数/双精度数加倍
        case b @ BinaryOperator(left, right) if left.dataType != right.dataType =>
          (left.dataType, right.dataType) match {
            case (t: IntegralType, DecimalType.Fixed(p, s)) =>
              b.makeCopy(Array(Cast(left, DecimalType.forType(t)), right))
            case (DecimalType.Fixed(p, s), t: IntegralType) =>
              b.makeCopy(Array(left, Cast(right, DecimalType.forType(t))))
            case (t, DecimalType.Fixed(p, s)) if isFloat(t) =>
              b.makeCopy(Array(left, Cast(right, DoubleType)))
            case (DecimalType.Fixed(p, s), t) if isFloat(t) =>
              b.makeCopy(Array(Cast(left, DoubleType), right))
            case _ =>
              b
          }

        // TODO: MaxOf, MinOf, etc might want other rules

        // SUM and AVERAGE are handled by the implementations of those expressions
      }
    }
  }

  /**
   * Changes numeric values to booleans so that expressions like true = 1 can be evaluated.
    * 将数值更改为布尔值,以便可以计算类似true = 1的表达式
   */
  object BooleanEquality extends Rule[LogicalPlan] {
    private val trueValues = Seq(1.toByte, 1.toShort, 1, 1L, Decimal.ONE)
    private val falseValues = Seq(0.toByte, 0.toShort, 0, 0L, Decimal.ZERO)

    private def buildCaseKeyWhen(booleanExpr: Expression, numericExpr: Expression) = {
      CaseKeyWhen(numericExpr, Seq(
        Literal(trueValues.head), booleanExpr,
        Literal(falseValues.head), Not(booleanExpr),
        Literal(false)))
    }

    private def transform(booleanExpr: Expression, numericExpr: Expression) = {
      If(Or(IsNull(booleanExpr), IsNull(numericExpr)),
        Literal.create(null, BooleanType),
        buildCaseKeyWhen(booleanExpr, numericExpr))
    }

    private def transformNullSafe(booleanExpr: Expression, numericExpr: Expression) = {
      CaseWhen(Seq(
        And(IsNull(booleanExpr), IsNull(numericExpr)), Literal(true),
        Or(IsNull(booleanExpr), IsNull(numericExpr)), Literal(false),
        buildCaseKeyWhen(booleanExpr, numericExpr)
      ))
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
      // Skip nodes who's children have not been resolved yet.
      //跳过那些子尚未解决的节点。
      case e if !e.childrenResolved => e

      // Hive treats (true = 1) as true and (false = 0) as true,
      // all other cases are considered as false.

      // We may simplify the expression if one side is literal numeric values
        //如果一边是文字数值,我们可以简化表达式
      case EqualTo(bool @ BooleanType(), Literal(value, _: NumericType))
        if trueValues.contains(value) => bool
      case EqualTo(bool @ BooleanType(), Literal(value, _: NumericType))
        if falseValues.contains(value) => Not(bool)
      case EqualTo(Literal(value, _: NumericType), bool @ BooleanType())
        if trueValues.contains(value) => bool
      case EqualTo(Literal(value, _: NumericType), bool @ BooleanType())
        if falseValues.contains(value) => Not(bool)
      case EqualNullSafe(bool @ BooleanType(), Literal(value, _: NumericType))
        if trueValues.contains(value) => And(IsNotNull(bool), bool)
      case EqualNullSafe(bool @ BooleanType(), Literal(value, _: NumericType))
        if falseValues.contains(value) => And(IsNotNull(bool), Not(bool))
      case EqualNullSafe(Literal(value, _: NumericType), bool @ BooleanType())
        if trueValues.contains(value) => And(IsNotNull(bool), bool)
      case EqualNullSafe(Literal(value, _: NumericType), bool @ BooleanType())
        if falseValues.contains(value) => And(IsNotNull(bool), Not(bool))

      case EqualTo(left @ BooleanType(), right @ NumericType()) =>
        transform(left , right)
      case EqualTo(left @ NumericType(), right @ BooleanType()) =>
        transform(right, left)
      case EqualNullSafe(left @ BooleanType(), right @ NumericType()) =>
        transformNullSafe(left, right)
      case EqualNullSafe(left @ NumericType(), right @ BooleanType()) =>
        transformNullSafe(right, left)
    }
  }

  /**
   * When encountering a cast from a string representing a valid fractional number to an integral
   * type the jvm will throw a `java.lang.NumberFormatException`.  Hive, in contrast, returns the
   * truncated version of this number.
   */
  object StringToIntegralCasts extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      case Cast(e @ StringType(), t: IntegralType) =>
        Cast(Cast(e, DecimalType.forType(LongType)), t)
    }
  }

  /**
   * This ensure that the types for various functions are as expected.
    * 这可确保各种功能的类型符合预期
   */
  object FunctionArgumentConversion extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      case a @ CreateArray(children) if children.map(_.dataType).distinct.size > 1 =>
        val types = children.map(_.dataType)
        findTightestCommonTypeAndPromoteToString(types) match {
          case Some(finalDataType) => CreateArray(children.map(Cast(_, finalDataType)))
          case None => a
        }

      // Promote SUM, SUM DISTINCT and AVERAGE to largest types to prevent overflows.
        //将SUM,SUM DISTINCT和AVERAGE提升为最大类型以防止溢出
      case s @ Sum(e @ DecimalType()) => s // Decimal is already the biggest.
      case Sum(e @ IntegralType()) if e.dataType != LongType => Sum(Cast(e, LongType))
      case Sum(e @ FractionalType()) if e.dataType != DoubleType => Sum(Cast(e, DoubleType))

      case s @ SumDistinct(e @ DecimalType()) => s // Decimal is already the biggest.
      case SumDistinct(e @ IntegralType()) if e.dataType != LongType =>
        SumDistinct(Cast(e, LongType))
      case SumDistinct(e @ FractionalType()) if e.dataType != DoubleType =>
        SumDistinct(Cast(e, DoubleType))

      case s @ Average(e @ DecimalType()) => s // Decimal is already the biggest.
      case Average(e @ IntegralType()) if e.dataType != LongType =>
        Average(Cast(e, LongType))
      case Average(e @ FractionalType()) if e.dataType != DoubleType =>
        Average(Cast(e, DoubleType))

      // Hive lets you do aggregation of timestamps... for some reason
        //Hive允许您汇总时间戳...出于某种原因
      case Sum(e @ TimestampType()) => Sum(Cast(e, DoubleType))
      case Average(e @ TimestampType()) => Average(Cast(e, DoubleType))

      // Coalesce should return the first non-null value, which could be any column
      // from the list. So we need to make sure the return type is deterministic and
      // compatible with every child column.
        //Coalesce应返回第一个非null值,该值可以是列表中的任何列,因此,我们需要确保返回类型是确定性的并且与每个子列兼容。
      case c @ Coalesce(es) if es.map(_.dataType).distinct.size > 1 =>
        val types = es.map(_.dataType)
        findWiderCommonType(types) match {
          case Some(finalDataType) => Coalesce(es.map(Cast(_, finalDataType)))
          case None => c
        }

      case NaNvl(l, r) if l.dataType == DoubleType && r.dataType == FloatType =>
        NaNvl(l, Cast(r, DoubleType))
      case NaNvl(l, r) if l.dataType == FloatType && r.dataType == DoubleType =>
        NaNvl(Cast(l, DoubleType), r)
    }
  }

  /**
   * Hive only performs integral division with the DIV operator. The arguments to / are always
   * converted to fractional types.
    * Hive仅与DIV运算符执行整数除法,参数总是转换为小数类型
   */
  object Division extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
      // Skip nodes who has not been resolved yet,
      // as this is an extra rule which should be applied at last.
      case e if !e.resolved => e

      // Decimal and Double remain the same
        //十进制和双数保持不变
      case d: Divide if d.dataType == DoubleType => d
      case d: Divide if d.dataType.isInstanceOf[DecimalType] => d

      case Divide(left, right) => Divide(Cast(left, DoubleType), Cast(right, DoubleType))
    }
  }

  /**
   * Coerces the type of different branches of a CASE WHEN statement to a common type.
    * 将CASE WHEN语句的不同分支的类型强制转换为公共类型
   */
  object CaseWhenCoercion extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
      case c: CaseWhenLike if c.childrenResolved && !c.valueTypesEqual =>
        logDebug(s"Input values for null casting ${c.valueTypes.mkString(",")}")
        val maybeCommonType = findWiderCommonType(c.valueTypes)
        maybeCommonType.map { commonType =>
          val castedBranches = c.branches.grouped(2).map {
            case Seq(when, value) if value.dataType != commonType =>
              Seq(when, Cast(value, commonType))
            case Seq(elseVal) if elseVal.dataType != commonType =>
              Seq(Cast(elseVal, commonType))
            case other => other
          }.reduce(_ ++ _)
          c match {
            case _: CaseWhen => CaseWhen(castedBranches)
            case CaseKeyWhen(key, _) => CaseKeyWhen(key, castedBranches)
          }
        }.getOrElse(c)

      case c: CaseKeyWhen if c.childrenResolved && !c.resolved =>
        val maybeCommonType =
          findWiderCommonType((c.key +: c.whenList).map(_.dataType))
        maybeCommonType.map { commonType =>
          val castedBranches = c.branches.grouped(2).map {
            case Seq(whenExpr, thenExpr) if whenExpr.dataType != commonType =>
              Seq(Cast(whenExpr, commonType), thenExpr)
            case other => other
          }.reduce(_ ++ _)
          CaseKeyWhen(Cast(c.key, commonType), castedBranches)
        }.getOrElse(c)
    }
  }

  /**
   * Coerces the type of different branches of If statement to a common type.
    * 将If语句的不同分支的类型强制转换为公共类型
   */
  object IfCoercion extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
      case e if !e.childrenResolved => e
      // Find tightest common type for If, if the true value and false value have different types.
      case i @ If(pred, left, right) if left.dataType != right.dataType =>
        findTightestCommonTypeToString(left.dataType, right.dataType).map { widestType =>
          val newLeft = if (left.dataType == widestType) left else Cast(left, widestType)
          val newRight = if (right.dataType == widestType) right else Cast(right, widestType)
          If(pred, newLeft, newRight)
        }.getOrElse(i)  // If there is no applicable conversion, leave expression unchanged.

      // Convert If(null literal, _, _) into boolean type.
      // In the optimizer, we should short-circuit this directly into false value.
      case If(pred, left, right) if pred.dataType == NullType =>
        If(Literal.create(null, BooleanType), left, right)
    }
  }

  /**
   * Turns Add/Subtract of DateType/TimestampType/StringType and CalendarIntervalType
   * to TimeAdd/TimeSub
   */
  object DateTimeOperations extends Rule[LogicalPlan] {

    private val acceptedTypes = Seq(DateType, TimestampType, StringType)

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      case Add(l @ CalendarIntervalType(), r) if acceptedTypes.contains(r.dataType) =>
        Cast(TimeAdd(r, l), r.dataType)
      case Add(l, r @ CalendarIntervalType()) if acceptedTypes.contains(l.dataType) =>
        Cast(TimeAdd(l, r), l.dataType)
      case Subtract(l, r @ CalendarIntervalType()) if acceptedTypes.contains(l.dataType) =>
        Cast(TimeSub(l, r), l.dataType)
    }
  }

  /**
   * Casts types according to the expected input types for [[Expression]]s.
    * 根据[[Expression]]的预期输入类型转换类型
   */
  object ImplicitTypeCasts extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
      // Skip nodes who's children have not been resolved yet.
      //跳过那些子尚未解决的节点
      case e if !e.childrenResolved => e

      case b @ BinaryOperator(left, right) if left.dataType != right.dataType =>
        findTightestCommonTypeOfTwo(left.dataType, right.dataType).map { commonType =>
          if (b.inputType.acceptsType(commonType)) {
            // If the expression accepts the tightest common type, cast to that.
            //如果表达式接受最紧密的常见类型,则转换为该类型
            val newLeft = if (left.dataType == commonType) left else Cast(left, commonType)
            val newRight = if (right.dataType == commonType) right else Cast(right, commonType)
            b.withNewChildren(Seq(newLeft, newRight))
          } else {
            // Otherwise, don't do anything with the expression.
            //否则,不要对表达式执行任何操作
            b
          }
          //如果没有适用的转换,请保持表达式不变
        }.getOrElse(b)  // If there is no applicable conversion, leave expression unchanged.

      case e: ImplicitCastInputTypes if e.inputTypes.nonEmpty =>
        val children: Seq[Expression] = e.children.zip(e.inputTypes).map { case (in, expected) =>
          // If we cannot do the implicit cast, just use the original input.
          //如果我们不能进行隐式转换,只需使用原始输入
          implicitCast(in, expected).getOrElse(in)
        }
        e.withNewChildren(children)

      case e: ExpectsInputTypes if e.inputTypes.nonEmpty =>
        // Convert NullType into some specific target type for ExpectsInputTypes that don't do
        // general implicit casting.
        //将NullType转换为不执行常规隐式转换的ExpectsInputTypes的某个特定目标类型
        val children: Seq[Expression] = e.children.zip(e.inputTypes).map { case (in, expected) =>
          if (in.dataType == NullType && !expected.acceptsType(NullType)) {
            Literal.create(null, expected.defaultConcreteType)
          } else {
            in
          }
        }
        e.withNewChildren(children)
    }

    /**
     * Given an expected data type, try to cast the expression and return the cast expression.
      * 给定预期的数据类型,尝试转换表达式并返回强制转换表达式
     *
     * If the expression already fits the input type, we simply return the expression itself.
      * 如果表达式已经适合输入类型,我们只返回表达式本身
     * If the expression has an incompatible type that cannot be implicitly cast, return None.
      * 如果表达式具有无法隐式转换的不兼容类型,则返回None
     */
    def implicitCast(e: Expression, expectedType: AbstractDataType): Option[Expression] = {
      val inType = e.dataType

      // Note that ret is nullable to avoid typing a lot of Some(...) in this local scope.
      // We wrap immediately an Option after this.
      @Nullable val ret: Expression = (inType, expectedType) match {

        // If the expected type is already a parent of the input type, no need to cast.
          //如果期望的类型已经是输入类型的父类,则无需强制转换
        case _ if expectedType.acceptsType(inType) => e

        // Cast null type (usually from null literals) into target types
          //将null类型（通常从null文字）转换为目标类型
        case (NullType, target) => Cast(e, target.defaultConcreteType)

        // If the function accepts any numeric type and the input is a string, we follow the hive
        // convention and cast that input into a double
          //如果函数接受任何数字类型并且输入是字符串,我们遵循hive约定并将该输入转换为double
        case (StringType, NumericType) => Cast(e, NumericType.defaultConcreteType)

        // Implicit cast among numeric types. When we reach here, input type is not acceptable.
        //数字类型之间的隐式转换,当我们到达这里时,输入类型是不可接受的
        // If input is a numeric type but not decimal, and we expect a decimal type,
        // cast the input to decimal.
          //如果输入是数字类型但不是十进制,并且我们期望十进制类型,则将输入转换为十进制
        case (d: NumericType, DecimalType) => Cast(e, DecimalType.forType(d))
        // For any other numeric types, implicitly cast to each other, e.g. long -> int, int -> long
          //对于任何其他数字类型,隐式地相互投射,例如, long - > int,int - > long
        case (_: NumericType, target: NumericType) => Cast(e, target)

        // Implicit cast between date time types 日期时间类型之间的隐式转换
        case (DateType, TimestampType) => Cast(e, TimestampType)
        case (TimestampType, DateType) => Cast(e, DateType)

        // Implicit cast from/to string 从/到字符串的隐式强制转换
        case (StringType, DecimalType) => Cast(e, DecimalType.SYSTEM_DEFAULT)
        case (StringType, target: NumericType) => Cast(e, target)
        case (StringType, DateType) => Cast(e, DateType)
        case (StringType, TimestampType) => Cast(e, TimestampType)
        case (StringType, BinaryType) => Cast(e, BinaryType)
        // Cast any atomic type to string. 将任何原子类型转换为字符串。
        case (any: AtomicType, StringType) if any != StringType => Cast(e, StringType)

        // When we reach here, input type is not acceptable for any types in this type collection,
        // try to find the first one we can implicitly cast.
          //当我们到达这里时,输入类型对于此类型集合中的任何类型都是不可接受的,尝试找到我们可以隐式转换的第一个类型
        case (_, TypeCollection(types)) => types.flatMap(implicitCast(e, _)).headOption.orNull

        // Else, just return the same input expression
          //否则,只返回相同的输入表达式
        case _ => null
      }
      Option(ret)
    }
  }
}
