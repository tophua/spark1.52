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

import java.io.CharArrayWriter
import java.util.Properties

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.util.control.NonFatal

import com.fasterxml.jackson.core.JsonFactory
import org.apache.commons.lang3.StringUtils

import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, ScalaReflection, SqlParser}
import org.apache.spark.sql.execution.{EvaluatePython, ExplainCommand, FileRelation, LogicalRDD, SQLExecution}
import org.apache.spark.sql.execution.datasources.{CreateTableUsingAsSelect, LogicalRelation}
import org.apache.spark.sql.execution.datasources.json.JacksonGenerator
import org.apache.spark.sql.sources.HadoopFsRelation
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils


private[sql] object DataFrame {
  def apply(sqlContext: SQLContext, logicalPlan: LogicalPlan): DataFrame = {
    new DataFrame(sqlContext, logicalPlan)
  }
}

/**
 * :: Experimental ::
 * A distributed collection of data organized into named columns.
  * 分布在命名列中的分布式数据集合
 *
 * A [[DataFrame]] is equivalent to a relational table in Spark SQL. The following example creates
 * a [[DataFrame]] by pointing Spark SQL to a Parquet data set.
  * [[DataFrame]]等效于Spark SQL中的关系表,以下示例通过将Spark SQL指向Parquet数据集来创建[[DataFrame]]
 * {{{
 *   val people = sqlContext.read.parquet("...")  // in Scala
 *   DataFrame people = sqlContext.read().parquet("...")  // in Java
 * }}}
 *
 * Once created, it can be manipulated using the various domain-specific-language (DSL) functions
 * defined in: [[DataFrame]] (this class), [[Column]], and [[functions]].
  *
  * 一旦创建,就可以使用在[[DataFrame]]（此类）,
  * [[Column]]和[[functions]]中定义的各种特定于域的语言（DSL）函数来操作它。
 *
 * To select a column from the data frame, use `apply` method in Scala and `col` in Java.
 * {{{
 *   val ageCol = people("age")  // in Scala
 *   Column ageCol = people.col("age")  // in Java
 * }}}
 *
 * Note that the [[Column]] type can also be manipulated through its various functions.
 * {{{
 *   // The following creates a new column that increases everybody's age by 10.
 *   people("age") + 10  // in Scala
 *   people.col("age").plus(10);  // in Java
 * }}}
 *
 * A more concrete example in Scala:
 * {{{
 *   // To create DataFrame using SQLContext
 *   val people = sqlContext.read.parquet("...")
 *   val department = sqlContext.read.parquet("...")
 *
 *   people.filter("age > 30")
 *     .join(department, people("deptId") === department("id"))
 *     .groupBy(department("name"), "gender")
 *     .agg(avg(people("salary")), max(people("age")))
 * }}}
 *
 * and in Java:
 * {{{
 *   // To create DataFrame using SQLContext
 *   DataFrame people = sqlContext.read().parquet("...");
 *   DataFrame department = sqlContext.read().parquet("...");
 *
 *   people.filter("age".gt(30))
 *     .join(department, people.col("deptId").equalTo(department("id")))
 *     .groupBy(department.col("name"), "gender")
 *     .agg(avg(people.col("salary")), max(people.col("age")));
 * }}}
 *
 * @groupname basic Basic DataFrame functions
 * @groupname dfops Language Integrated Queries
 * @groupname rdd RDD Operations
 * @groupname output Output Operations
 * @groupname action Actions
 * @since 1.3.0
 */
// TODO: Improve documentation.
@Experimental
class DataFrame private[sql](
    @transient val sqlContext: SQLContext,
    @DeveloperApi @transient val queryExecution: SQLContext#QueryExecution) extends Serializable {

  // Note for Spark contributors: if adding or updating any action in `DataFrame`, please make sure
  // you wrap it with `withNewExecutionId` if this actions doesn't call other action.
  //Spark贡献者的注意事项：如果在`DataFrame`中添加或更新任何操作,
  //请确保使用`withNewExecutionId`包装它,如果此操作不调用其他操作。
  /**
   * A constructor that automatically analyzes the logical plan.
   * 一个自动分析逻辑计划的构造函数
   * This reports error eagerly as the [[DataFrame]] is constructed, unless
   * [[SQLConf.dataFrameEagerAnalysis]] is turned off.
   */
  def this(sqlContext: SQLContext, logicalPlan: LogicalPlan) = {
    this(sqlContext, {
      val qe = sqlContext.executePlan(logicalPlan)
      if (sqlContext.conf.dataFrameEagerAnalysis) {
        qe.assertAnalyzed()  // This should force analysis and throw errors if there are any
      }
      qe
    })
  }

  @transient protected[sql] val logicalPlan: LogicalPlan = queryExecution.logical match {
    // For various commands (like DDL) and queries with side effects, we force query optimization to
    // happen right away to let these side effects take place eagerly.
    //对于各种命令（如DDL）和带副作用的查询,我们立即强制进行查询优化,以便让这些副作用急切发生。
    case _: Command |
         _: InsertIntoTable |
         _: CreateTableUsingAsSelect =>
      LogicalRDD(queryExecution.analyzed.output, queryExecution.toRdd)(sqlContext)
    case _ =>
      queryExecution.analyzed
  }

  /**
   * An implicit conversion function internal to this class for us to avoid doing
   * "new DataFrame(...)" everywhere.
    * 这个类内部的隐式转换函数,以避免在任何地方执行“new DataFrame（...）”
   */
  @inline private implicit def logicalPlanToDataFrame(logicalPlan: LogicalPlan): DataFrame = {
    new DataFrame(sqlContext, logicalPlan)
  }

  protected[sql] def resolve(colName: String): NamedExpression = {
    queryExecution.analyzed.resolveQuoted(colName, sqlContext.analyzer.resolver).getOrElse {
      throw new AnalysisException(
        s"""Cannot resolve column name "$colName" among (${schema.fieldNames.mkString(", ")})""")
    }
  }

  protected[sql] def numericColumns: Seq[Expression] = {
    schema.fields.filter(_.dataType.isInstanceOf[NumericType]).map { n =>
      queryExecution.analyzed.resolveQuoted(n.name, sqlContext.analyzer.resolver).get
    }
  }

  /**
   * Compose the string representing rows for output
    * 编写表示输出行的字符串
   * @param _numRows Number of rows to show
   * @param truncate Whether truncate long strings and align cells right
   */
  private[sql] def showString(_numRows: Int, truncate: Boolean = true): String = {
    val numRows = _numRows.max(0)
    val sb = new StringBuilder
    val takeResult = take(numRows + 1)
    val hasMoreData = takeResult.length > numRows
    val data = takeResult.take(numRows)
    val numCols = schema.fieldNames.length

    // For array values, replace Seq and Array with square brackets
    // For cells that are beyond 20 characters, replace it with the first 17 and "..."
    //对于数组值,使用方括号替换Seq和Array对于超过20个字符的单元格,将其替换为前17个和“...”
    val rows: Seq[Seq[String]] = schema.fieldNames.toSeq +: data.map { row =>
      row.toSeq.map { cell =>
        val str = cell match {
          case null => "null"
          case array: Array[_] => array.mkString("[", ", ", "]")
          case seq: Seq[_] => seq.mkString("[", ", ", "]")
          case _ => cell.toString
        }
        if (truncate && str.length > 20) str.substring(0, 17) + "..." else str
      }: Seq[String]
    }

    // Initialise the width of each column to a minimum value of '3'
    //将每列的宽度初始化为最小值“3”
    val colWidths = Array.fill(numCols)(3)

    // Compute the width of each column
    //计算每列的宽度
    for (row <- rows) {
      for ((cell, i) <- row.zipWithIndex) {
        colWidths(i) = math.max(colWidths(i), cell.length)
      }
    }

    // Create SeparateLine 创建SeparateLine
    val sep: String = colWidths.map("-" * _).addString(sb, "+", "+", "+\n").toString()

    // column names 列名
    rows.head.zipWithIndex.map { case (cell, i) =>
      if (truncate) {
        StringUtils.leftPad(cell, colWidths(i))
      } else {
        StringUtils.rightPad(cell, colWidths(i))
      }
    }.addString(sb, "|", "|", "|\n")

    sb.append(sep)

    // data
    rows.tail.map {
      _.zipWithIndex.map { case (cell, i) =>
        if (truncate) {
          StringUtils.leftPad(cell.toString, colWidths(i))
        } else {
          StringUtils.rightPad(cell.toString, colWidths(i))
        }
      }.addString(sb, "|", "|", "|\n")
    }

    sb.append(sep)

    // For Data that has more than "numRows" records
    //对于具有多于“numRows”记录的数据
    if (hasMoreData) {
      val rowsString = if (numRows == 1) "row" else "rows"
      sb.append(s"only showing top $numRows ${rowsString}\n")
    }

    sb.toString()
  }

  override def toString: String = {
    try {
      schema.map(f => s"${f.name}: ${f.dataType.simpleString}").mkString("[", ", ", "]")
    } catch {
      case NonFatal(e) =>
        s"Invalid tree; ${e.getMessage}:\n$queryExecution"
    }
  }

  /**
   * Returns the object itself.返回对象本身
   * @group basic
   * @since 1.3.0
   */
  // This is declared with parentheses to prevent the Scala compiler from treating
  // `rdd.toDF("1")` as invoking this toDF and then apply on the returned DataFrame.
  //这是用括号声明的,以防止Scala编译器将`rdd.toDF（“1”）`视为调用此toDF,然后应用于返回的DataFrame。
  def toDF(): DataFrame = this

  /**
   * Returns a new [[DataFrame]] with columns renamed. This can be quite convenient in conversion
   * from a RDD of tuples into a [[DataFrame]] with meaningful names. For example:
    * 返回重命名列的新[[DataFrame]],从元组的RDD转换为具有有意义名称的[[DataFrame]]可以非常方便, 例如：
   * {{{
   *   val rdd: RDD[(Int, String)] = ...
   *   rdd.toDF()  // this implicit conversion creates a DataFrame with column name _1 and _2
   *   rdd.toDF("id", "name")  // this creates a DataFrame with column name "id" and "name"
   * }}}
   * @group basic
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def toDF(colNames: String*): DataFrame = {
    require(schema.size == colNames.size,
      "The number of columns doesn't match.\n" +
        s"Old column names (${schema.size}): " + schema.fields.map(_.name).mkString(", ") + "\n" +
        s"New column names (${colNames.size}): " + colNames.mkString(", "))

    val newCols = logicalPlan.output.zip(colNames).map { case (oldAttribute, newName) =>
      Column(oldAttribute).as(newName)
    }
    select(newCols : _*)
  }

  /**
   * Returns the schema of this [[DataFrame]].
    * 返回此[[DataFrame]]的架构
   * @group basic
   * @since 1.3.0
   */
  def schema: StructType = queryExecution.analyzed.schema

  /**
   * Returns all column names and their data types as an array.
    * 将所有列名称及其数据类型作为数组返回
   * @group basic
   * @since 1.3.0
   */
  def dtypes: Array[(String, String)] = schema.fields.map { field =>
    (field.name, field.dataType.toString)
  }

  /**
   * Returns all column names as an array.
    * 将所有列名称作为数组返回
   * @group basic
   * @since 1.3.0
   */
  def columns: Array[String] = schema.fields.map(_.name)

  /**
   * Prints the schema to the console in a nice tree format.
    * 以良好的树格式将模式打印到控制台
   * @group basic
   * @since 1.3.0
   */
  // scalastyle:off println
  def printSchema(): Unit = println(schema.treeString)
  // scalastyle:on println

  /**
   * Prints the plans (logical and physical) to the console for debugging purposes.
    * 将计划（逻辑和物理）打印到控制台以进行调试
   * @group basic
   * @since 1.3.0
   */
  def explain(extended: Boolean): Unit = {
    val explain = ExplainCommand(queryExecution.logical, extended = extended)
    explain.queryExecution.executedPlan.executeCollect().foreach {
      // scalastyle:off println
      r => println(r.getString(0))
      // scalastyle:on println
    }
  }

  /**
   * Only prints the physical plan to the console for debugging purposes.
    * 仅将物理计划打印到控制台以进行调试
   * @group basic
   * @since 1.3.0
   */
  def explain(): Unit = explain(extended = false)

  /**
   * Returns true if the `collect` and `take` methods can be run locally
    * 如果`collect`和`take`方法可以在本地运行,则返回true
   * (without any Spark executors).
   * @group basic
   * @since 1.3.0
   */
  def isLocal: Boolean = logicalPlan.isInstanceOf[LocalRelation]

  /**
   * Displays the [[DataFrame]] in a tabular form. Strings more than 20 characters will be
   * truncated, and all cells will be aligned right. For example:
    * 以表格形式显示[[DataFrame]],超过20个字符的字符串将被截断,并且所有单元格将对齐。 例如：
   * {{{
   *   year  month AVG('Adj Close) MAX('Adj Close)
   *   1980  12    0.503218        0.595103
   *   1981  01    0.523289        0.570307
   *   1982  02    0.436504        0.475256
   *   1983  03    0.410516        0.442194
   *   1984  04    0.450090        0.483521
   * }}}
   * @param numRows Number of rows to show
   *
   * @group action
   * @since 1.3.0
   */
  def show(numRows: Int): Unit = show(numRows, true)

  /**
   * Displays the top 20 rows of [[DataFrame]] in a tabular form. Strings more than 20 characters
   * will be truncated, and all cells will be aligned right.
    * 以表格形式显示[[DataFrame]]的前20行,超过20个字符的字符串将被截断,并且所有单元格将对齐。
   * @group action
   * @since 1.3.0
   */
  def show(): Unit = show(20)

  /**
   * Displays the top 20 rows of [[DataFrame]] in a tabular form.
    * 以表格形式显示[[DataFrame]]的前20行
   *
   * @param truncate Whether truncate long strings. If true, strings more than 20 characters will
   *              be truncated and all cells will be aligned right
   *
   * @group action
   * @since 1.5.0
   */
  def show(truncate: Boolean): Unit = show(20, truncate)

  /**
   * Displays the [[DataFrame]] in a tabular form. For example:
    * 以表格形式显示[[DataFrame]]。 例如
   * {{{
   *   year  month AVG('Adj Close) MAX('Adj Close)
   *   1980  12    0.503218        0.595103
   *   1981  01    0.523289        0.570307
   *   1982  02    0.436504        0.475256
   *   1983  03    0.410516        0.442194
   *   1984  04    0.450090        0.483521
   * }}}
   * @param numRows Number of rows to show
   * @param truncate Whether truncate long strings. If true, strings more than 20 characters will
   *              be truncated and all cells will be aligned right
   *
   * @group action
   * @since 1.5.0
   */
  // scalastyle:off println
  def show(numRows: Int, truncate: Boolean): Unit = println(showString(numRows, truncate))
  // scalastyle:on println

  /**
   * Returns a [[DataFrameNaFunctions]] for working with missing data.
    * 返回用于处理缺失数据的[[DataFrameNaFunctions]]
   * {{{
   *   // Dropping rows containing any null values.
   *   df.na.drop()
   * }}}
   *
   * @group dfops
   * @since 1.3.1
   */
  def na: DataFrameNaFunctions = new DataFrameNaFunctions(this)

  /**
   * Returns a [[DataFrameStatFunctions]] for working statistic functions support.
    * 返回工作统计函数支持的[[DataFrameStatFunctions]]
   * {{{
   *   // Finding frequent items in column with name 'a'.
   *   df.stat.freqItems(Seq("a"))
   * }}}
   *
   * @group dfops
   * @since 1.4.0
   */
  def stat: DataFrameStatFunctions = new DataFrameStatFunctions(this)

  /**
   * Cartesian join with another [[DataFrame]].
    * 笛卡尔与另一个[[DataFrame]]连接
   *
   * Note that cartesian joins are very expensive without an extra filter that can be pushed down.
   *
   * @param right Right side of the join operation.
   * @group dfops
   * @since 1.3.0
   */
  def join(right: DataFrame): DataFrame = {
    Join(logicalPlan, right.logicalPlan, joinType = Inner, None)
  }

  /**
   * Inner equi-join with another [[DataFrame]] using the given column.
    * 使用给定列与另一个[[DataFrame]]进行内部等连接
   *
   * Different from other join functions, the join column will only appear once in the output,
   * i.e. similar to SQL's `JOIN USING` syntax.
   *
   * {{{
   *   // Joining df1 and df2 using the column "user_id"
   *   df1.join(df2, "user_id")
   * }}}
   *
   * Note that if you perform a self-join using this function without aliasing the input
   * [[DataFrame]]s, you will NOT be able to reference any columns after the join, since
   * there is no way to disambiguate which side of the join you would like to reference.
   *
   * @param right Right side of the join operation.
   * @param usingColumn Name of the column to join on. This column must exist on both sides.
   * @group dfops
   * @since 1.4.0
   */
  def join(right: DataFrame, usingColumn: String): DataFrame = {
    join(right, Seq(usingColumn))
  }

  /**
   * Inner equi-join with another [[DataFrame]] using the given columns.
    * 使用给定列与另一个[[DataFrame]]进行内部等连接
   *
   * Different from other join functions, the join columns will only appear once in the output,
   * i.e. similar to SQL's `JOIN USING` syntax.
   *
   * {{{
   *   // Joining df1 and df2 using the columns "user_id" and "user_name"
   *   df1.join(df2, Seq("user_id", "user_name"))
   * }}}
   *
   * Note that if you perform a self-join using this function without aliasing the input
   * [[DataFrame]]s, you will NOT be able to reference any columns after the join, since
   * there is no way to disambiguate which side of the join you would like to reference.
   *
   * @param right Right side of the join operation.
   * @param usingColumns Names of the columns to join on. This columns must exist on both sides.
   * @group dfops
   * @since 1.4.0
   */
  def join(right: DataFrame, usingColumns: Seq[String]): DataFrame = {
    // Analyze the self join. The assumption is that the analyzer will disambiguate left vs right
    // by creating a new instance for one of the branch.
    val joined = sqlContext.executePlan(
      Join(logicalPlan, right.logicalPlan, joinType = Inner, None)).analyzed.asInstanceOf[Join]

    // Project only one of the join columns.
    val joinedCols = usingColumns.map(col => joined.right.resolve(col))
    val condition = usingColumns.map { col =>
      catalyst.expressions.EqualTo(joined.left.resolve(col), joined.right.resolve(col))
    }.reduceLeftOption[catalyst.expressions.BinaryExpression] { (cond, eqTo) =>
      catalyst.expressions.And(cond, eqTo)
    }

    Project(
      joined.output.filterNot(joinedCols.contains(_)),
      Join(
        joined.left,
        joined.right,
        joinType = Inner,
        condition)
    )
  }

  /**
   * Inner join with another [[DataFrame]], using the given join expression.
    * 使用给定的连接表达式与另一个[[DataFrame]]进行内连接
   *
   * {{{
   *   // The following two are equivalent:
   *   df1.join(df2, $"df1Key" === $"df2Key")
   *   df1.join(df2).where($"df1Key" === $"df2Key")
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  def join(right: DataFrame, joinExprs: Column): DataFrame = join(right, joinExprs, "inner")

  /**
   * Join with another [[DataFrame]], using the given join expression. The following performs
   * a full outer join between `df1` and `df2`.
    * 使用给定的连接表达式加入另一个[[DataFrame]],以下内容在`df1`和`df2`之间执行完全外连接。
   *
   * {{{
   *   // Scala:
   *   import org.apache.spark.sql.functions._
   *   df1.join(df2, $"df1Key" === $"df2Key", "outer")
   *
   *   // Java:
   *   import static org.apache.spark.sql.functions.*;
   *   df1.join(df2, col("df1Key").equalTo(col("df2Key")), "outer");
   * }}}
   *
   * @param right Right side of the join.
   * @param joinExprs Join expression.
   * @param joinType One of: `inner`, `outer`, `left_outer`, `right_outer`, `leftsemi`.
   * @group dfops
   * @since 1.3.0
   */
  def join(right: DataFrame, joinExprs: Column, joinType: String): DataFrame = {
    // Note that in this function, we introduce a hack in the case of self-join to automatically
    // resolve ambiguous join conditions into ones that might make sense [SPARK-6231].
    // Consider this case: df.join(df, df("key") === df("key"))
    // Since df("key") === df("key") is a trivially true condition, this actually becomes a
    // cartesian join. However, most likely users expect to perform a self join using "key".
    // With that assumption, this hack turns the trivially true condition into equality on join
    // keys that are resolved to both sides.

    // Trigger analysis so in the case of self-join, the analyzer will clone the plan.
    // After the cloning, left and right side will have distinct expression ids.
    val plan = Join(logicalPlan, right.logicalPlan, JoinType(joinType), Some(joinExprs.expr))
      .queryExecution.analyzed.asInstanceOf[Join]

    // If auto self join alias is disabled, return the plan.
    //如果禁用了自动加入别名,则返回计划
    if (!sqlContext.conf.dataFrameSelfJoinAutoResolveAmbiguity) {
      return plan
    }

    // If left/right have no output set intersection, return the plan.
    //如果左/右没有输出设置交集,则返回计划
    val lanalyzed = this.logicalPlan.queryExecution.analyzed
    val ranalyzed = right.logicalPlan.queryExecution.analyzed
    if (lanalyzed.outputSet.intersect(ranalyzed.outputSet).isEmpty) {
      return plan
    }

    // Otherwise, find the trivially true predicates and automatically resolves them to both sides.
    // By the time we get here, since we have already run analysis, all attributes should've been
    // resolved and become AttributeReference.
    val cond = plan.condition.map { _.transform {
      case catalyst.expressions.EqualTo(a: AttributeReference, b: AttributeReference)
          if a.sameRef(b) =>
        catalyst.expressions.EqualTo(plan.left.resolve(a.name), plan.right.resolve(b.name))
    }}
    plan.copy(condition = cond)
  }

  /**
   * Returns a new [[DataFrame]] sorted by the specified column, all in ascending order.
    * 返回按指定列排序的新[[DataFrame]],全部按升序排列
   * {{{
   *   // The following 3 are equivalent
   *   df.sort("sortcol")
   *   df.sort($"sortcol")
   *   df.sort($"sortcol".asc)
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def sort(sortCol: String, sortCols: String*): DataFrame = {
    sort((sortCol +: sortCols).map(apply) : _*)
  }

  /**
   * Returns a new [[DataFrame]] sorted by the given expressions. For example:
    * 返回按给定表达式排序的新[[DataFrame]]。 例如：
   * {{{
   *   df.sort($"col1", $"col2".desc)
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def sort(sortExprs: Column*): DataFrame = {
    val sortOrder: Seq[SortOrder] = sortExprs.map { col =>
      col.expr match {
        case expr: SortOrder =>
          expr
        case expr: Expression =>
          SortOrder(expr, Ascending)
      }
    }
    Sort(sortOrder, global = true, logicalPlan)
  }

  /**
   * Returns a new [[DataFrame]] sorted by the given expressions.
    * 返回按给定表达式排序的新[[DataFrame]]
   * This is an alias of the `sort` function.
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def orderBy(sortCol: String, sortCols: String*): DataFrame = sort(sortCol, sortCols : _*)

  /**
   * Returns a new [[DataFrame]] sorted by the given expressions.
    * 返回按给定表达式排序的新[[DataFrame]]
   * This is an alias of the `sort` function.
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def orderBy(sortExprs: Column*): DataFrame = sort(sortExprs : _*)

  /**
   * Selects column based on the column name and return it as a [[Column]].
    * 根据列名选择列并将其作为[[Column]]返回
   * Note that the column name can also reference to a nested column like `a.b`.
   * @group dfops
   * @since 1.3.0
   */
  def apply(colName: String): Column = col(colName)

  /**
   * Selects column based on the column name and return it as a [[Column]].
    * 根据列名选择列并将其作为[[Column]]返回
   * Note that the column name can also reference to a nested column like `a.b`.
   * @group dfops
   * @since 1.3.0
   */
  def col(colName: String): Column = colName match {
    case "*" =>
      Column(ResolvedStar(schema.fieldNames.map(resolve)))
    case _ =>
      val expr = resolve(colName)
      Column(expr)
  }

  /**
   * Returns a new [[DataFrame]] with an alias set.
    * 返回带有别名集的新[[DataFrame]]
   * @group dfops
   * @since 1.3.0
   */
  def as(alias: String): DataFrame = Subquery(alias, logicalPlan)

  /**
   * (Scala-specific) Returns a new [[DataFrame]] with an alias set.
   * @group dfops
   * @since 1.3.0
   */
  def as(alias: Symbol): DataFrame = as(alias.name)

  /**
   * Selects a set of column based expressions.
    * 选择一组基于列的表达式
   * {{{
   *   df.select($"colA", $"colB" + 1)
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def select(cols: Column*): DataFrame = {
    val namedExpressions = cols.map {
      // Wrap UnresolvedAttribute with UnresolvedAlias, as when we resolve UnresolvedAttribute, we
      // will remove intermediate Alias for ExtractValue chain, and we need to alias it again to
      // make it a NamedExpression.
      case Column(u: UnresolvedAttribute) => UnresolvedAlias(u)
      case Column(expr: NamedExpression) => expr
      // Leave an unaliased explode with an empty list of names since the analyzer will generate the
      // correct defaults after the nested expression's type has been resolved.
      //Nil是一个空的List,::向队列的头部追加数据,创造新的列表
      case Column(explode: Explode) => MultiAlias(explode, Nil)
      case Column(expr: Expression) => Alias(expr, expr.prettyString)()
    }
    Project(namedExpressions.toSeq, logicalPlan)
  }

  /**
   * Selects a set of columns. This is a variant of `select` that can only select
   * existing columns using column names (i.e. cannot construct expressions).
    * 选择一组列,这是`select`的变体,只能使用列名选择现有列（即不能构造表达式）。
   *
   * {{{
   *   // The following two are equivalent:
   *   df.select("colA", "colB")
   *   df.select($"colA", $"colB")
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def select(col: String, cols: String*): DataFrame = select((col +: cols).map(Column(_)) : _*)

  /**
   * Selects a set of SQL expressions. This is a variant of `select` that accepts
    * 选择一组SQL表达式,这是接受的`select`的变体
   * SQL expressions.
   *
   * {{{
   *   df.selectExpr("colA", "colB as newName", "abs(colC)")
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def selectExpr(exprs: String*): DataFrame = {
    select(exprs.map { expr =>
      Column(SqlParser.parseExpression(expr))
    }: _*)
  }

  /**
   * Filters rows using the given condition.
    * 使用给定条件过滤行
   * {{{
   *   // The following are equivalent:
   *   peopleDf.filter($"age" > 15)
   *   peopleDf.where($"age" > 15)
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  def filter(condition: Column): DataFrame = Filter(condition.expr, logicalPlan)

  /**
   * Filters rows using the given SQL expression.
    * 使用给定的SQL表达式过滤行
   * {{{
   *   peopleDf.filter("age > 15")
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  def filter(conditionExpr: String): DataFrame = {
    filter(Column(SqlParser.parseExpression(conditionExpr)))
  }

  /**
   * Filters rows using the given condition. This is an alias for `filter`.
    * 使用给定条件过滤行,这是`filter`的别名
   * {{{
   *   // The following are equivalent:
   *   peopleDf.filter($"age" > 15)
   *   peopleDf.where($"age" > 15)
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  def where(condition: Column): DataFrame = filter(condition)

  /**
   * Filters rows using the given SQL expression.
    * 使用给定的SQL表达式过滤行
   * {{{
   *   peopleDf.where("age > 15")
   * }}}
   * @group dfops
   * @since 1.5.0
   */
  def where(conditionExpr: String): DataFrame = {
    filter(Column(SqlParser.parseExpression(conditionExpr)))
  }

  /**
   * Groups the [[DataFrame]] using the specified columns, so we can run aggregation on them.
    * 使用指定的列对[[DataFrame]]进行分组，因此我们可以对它们进行聚合
   * See [[GroupedData]] for all the available aggregate functions.
   *
   * {{{
   *   // Compute the average for all numeric columns grouped by department.
   *   df.groupBy($"department").avg()
   *
   *   // Compute the max age and average salary, grouped by department and gender.
   *   df.groupBy($"department", $"gender").agg(Map(
   *     "salary" -> "avg",
   *     "age" -> "max"
   *   ))
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def groupBy(cols: Column*): GroupedData = {
    GroupedData(this, cols.map(_.expr), GroupedData.GroupByType)
  }

  /**
   * Create a multi-dimensional rollup for the current [[DataFrame]] using the specified columns,
    * 使用指定的列为当前[[DataFrame]]创建多维汇总
   * so we can run aggregation on them.
   * See [[GroupedData]] for all the available aggregate functions.
   *
   * {{{
   *   // Compute the average for all numeric columns rolluped by department and group.
   *   df.rollup($"department", $"group").avg()
   *
   *   // Compute the max age and average salary, rolluped by department and gender.
   *   df.rollup($"department", $"gender").agg(Map(
   *     "salary" -> "avg",
   *     "age" -> "max"
   *   ))
   * }}}
   * @group dfops
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def rollup(cols: Column*): GroupedData = {
    GroupedData(this, cols.map(_.expr), GroupedData.RollupType)
  }

  /**
   * Create a multi-dimensional cube for the current [[DataFrame]] using the specified columns,
    * 使用指定的列为当前[[DataFrame]]创建多维立方体
   * so we can run aggregation on them.
   * See [[GroupedData]] for all the available aggregate functions.
   *
   * {{{
   *   // Compute the average for all numeric columns cubed by department and group.
   *   df.cube($"department", $"group").avg()
   *
   *   // Compute the max age and average salary, cubed by department and gender.
   *   df.cube($"department", $"gender").agg(Map(
   *     "salary" -> "avg",
   *     "age" -> "max"
   *   ))
   * }}}
   * @group dfops
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def cube(cols: Column*): GroupedData = GroupedData(this, cols.map(_.expr), GroupedData.CubeType)

  /**
   * Groups the [[DataFrame]] using the specified columns, so we can run aggregation on them.
    * 使用指定的列对[[DataFrame]]进行分组，因此我们可以对它们进行聚合
   * See [[GroupedData]] for all the available aggregate functions.
   *
   * This is a variant of groupBy that can only group by existing columns using column names
   * (i.e. cannot construct expressions).
   *
   * {{{
   *   // Compute the average for all numeric columns grouped by department.
   *   df.groupBy("department").avg()
   *
   *   // Compute the max age and average salary, grouped by department and gender.
   *   df.groupBy($"department", $"gender").agg(Map(
   *     "salary" -> "avg",
   *     "age" -> "max"
   *   ))
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def groupBy(col1: String, cols: String*): GroupedData = {
    val colNames: Seq[String] = col1 +: cols
    GroupedData(this, colNames.map(colName => resolve(colName)), GroupedData.GroupByType)
  }

  /**
   * Create a multi-dimensional rollup for the current [[DataFrame]] using the specified columns,
    * 使用指定的列为当前[[DataFrame]]创建多维汇总
   * so we can run aggregation on them.
   * See [[GroupedData]] for all the available aggregate functions.
   *
   * This is a variant of rollup that can only group by existing columns using column names
   * (i.e. cannot construct expressions).
   *
   * {{{
   *   // Compute the average for all numeric columns rolluped by department and group.
   *   df.rollup("department", "group").avg()
   *
   *   // Compute the max age and average salary, rolluped by department and gender.
   *   df.rollup($"department", $"gender").agg(Map(
   *     "salary" -> "avg",
   *     "age" -> "max"
   *   ))
   * }}}
   * @group dfops
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def rollup(col1: String, cols: String*): GroupedData = {
    val colNames: Seq[String] = col1 +: cols
    GroupedData(this, colNames.map(colName => resolve(colName)), GroupedData.RollupType)
  }

  /**
   * Create a multi-dimensional cube for the current [[DataFrame]] using the specified columns,
    * 使用指定的列为当前[[DataFrame]]创建多维立方体
   * so we can run aggregation on them.
   * See [[GroupedData]] for all the available aggregate functions.
   *
   * This is a variant of cube that can only group by existing columns using column names
    * 这是多维数据集的变体,只能使用列名称按现有列进行分组
   * (i.e. cannot construct expressions).
   *
   * {{{
   *   // Compute the average for all numeric columns cubed by department and group.
   *   df.cube("department", "group").avg()
   *
   *   // Compute the max age and average salary, cubed by department and gender.
   *   df.cube($"department", $"gender").agg(Map(
   *     "salary" -> "avg",
   *     "age" -> "max"
   *   ))
   * }}}
   * @group dfops
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def cube(col1: String, cols: String*): GroupedData = {
    val colNames: Seq[String] = col1 +: cols
    GroupedData(this, colNames.map(colName => resolve(colName)), GroupedData.CubeType)
  }

  /**
   * (Scala-specific) Aggregates on the entire [[DataFrame]] without groups.
    * （特定于Scala）在没有组的整个[[DataFrame]]上聚合
   * {{{
   *   // df.agg(...) is a shorthand for df.groupBy().agg(...)
   *   df.agg("age" -> "max", "salary" -> "avg")
   *   df.groupBy().agg("age" -> "max", "salary" -> "avg")
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  def agg(aggExpr: (String, String), aggExprs: (String, String)*): DataFrame = {
    groupBy().agg(aggExpr, aggExprs : _*)
  }

  /**
   * (Scala-specific) Aggregates on the entire [[DataFrame]] without groups.
    * （特定于Scala）在没有组的整个[[DataFrame]]上聚合
   * {{{
   *   // df.agg(...) is a shorthand for df.groupBy().agg(...)
   *   df.agg(Map("age" -> "max", "salary" -> "avg"))
   *   df.groupBy().agg(Map("age" -> "max", "salary" -> "avg"))
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  def agg(exprs: Map[String, String]): DataFrame = groupBy().agg(exprs)

  /**
   * (Java-specific) Aggregates on the entire [[DataFrame]] without groups.
   * {{{
   *   // df.agg(...) is a shorthand for df.groupBy().agg(...)
   *   df.agg(Map("age" -> "max", "salary" -> "avg"))
   *   df.groupBy().agg(Map("age" -> "max", "salary" -> "avg"))
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  def agg(exprs: java.util.Map[String, String]): DataFrame = groupBy().agg(exprs)

  /**
   * Aggregates on the entire [[DataFrame]] without groups.
    * 在没有组的情况下聚合整个[[DataFrame]]
   * {{{
   *   // df.agg(...) is a shorthand for df.groupBy().agg(...)
   *   df.agg(max($"age"), avg($"salary"))
   *   df.groupBy().agg(max($"age"), avg($"salary"))
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def agg(expr: Column, exprs: Column*): DataFrame = groupBy().agg(expr, exprs : _*)

  /**
   * Returns a new [[DataFrame]] by taking the first `n` rows. The difference between this function
   * and `head` is that `head` returns an array while `limit` returns a new [[DataFrame]].
    * 通过获取第一个“n”行返回一个新的[[DataFrame]],
    * 这个函数和`head`之间的区别在于`head`返回一个数组,而`limit`返回一个新的[[DataFrame]]
   * @group dfops
   * @since 1.3.0
   */
  def limit(n: Int): DataFrame = Limit(Literal(n), logicalPlan)

  /**
   * Returns a new [[DataFrame]] containing union of rows in this frame and another frame.
    * 返回包含此帧和另一帧中行的并集的新[[DataFrame]]
   * This is equivalent to `UNION ALL` in SQL.
   * @group dfops
   * @since 1.3.0
   */
  def unionAll(other: DataFrame): DataFrame = Union(logicalPlan, other.logicalPlan)

  /**
   * Returns a new [[DataFrame]] containing rows only in both this frame and another frame.
    * 返回仅在此frame和另一frame中包含行的新[[DataFrame]]
   * This is equivalent to `INTERSECT` in SQL.
   * @group dfops
   * @since 1.3.0
   */
  def intersect(other: DataFrame): DataFrame = Intersect(logicalPlan, other.logicalPlan)

  /**
   * Returns a new [[DataFrame]] containing rows in this frame but not in another frame.
   * This is equivalent to `EXCEPT` in SQL.
   * EXCEPT 返回两个结果集的差,(即从左查询中返回右查询没有找到的所有非重复值)
   * @group dfops
   * @since 1.3.0
   */
  def except(other: DataFrame): DataFrame = Except(logicalPlan, other.logicalPlan)

  /**
   * Returns a new [[DataFrame]] by sampling a fraction of rows.
    * 通过对一小部分行进行采样来返回一个新的[[DataFrame]]
   *
   * @param withReplacement Sample with replacement or not.
   * @param fraction Fraction of rows to generate.
   * @param seed Seed for sampling.
   * @group dfops
   * @since 1.3.0
   */
  def sample(withReplacement: Boolean, fraction: Double, seed: Long): DataFrame = {
    Sample(0.0, fraction, withReplacement, seed, logicalPlan)
  }

  /**
   * Returns a new [[DataFrame]] by sampling a fraction of rows, using a random seed.
    * 通过使用随机种子对一小部分行进行采样,返回一个新的[[DataFrame]]
   *
   * @param withReplacement Sample with replacement or not.
   * @param fraction Fraction of rows to generate.
   * @group dfops
   * @since 1.3.0
   */
  def sample(withReplacement: Boolean, fraction: Double): DataFrame = {
    sample(withReplacement, fraction, Utils.random.nextLong)
  }

  /**
   * Randomly splits this [[DataFrame]] with the provided weights.
    * 使用提供的权重随机分割此[[DataFrame]]
   *
   * @param weights weights for splits, will be normalized if they don't sum to 1.
   * @param seed Seed for sampling.
   * @group dfops
   * @since 1.4.0
   */
  def randomSplit(weights: Array[Double], seed: Long): Array[DataFrame] = {
    val sum = weights.sum
    val normalizedCumWeights = weights.map(_ / sum).scanLeft(0.0d)(_ + _)
    normalizedCumWeights.sliding(2).map { x =>
      new DataFrame(sqlContext, Sample(x(0), x(1), false, seed, logicalPlan))
    }.toArray
  }

  /**
   * Randomly splits this [[DataFrame]] with the provided weights.
    * 使用提供的权重随机分割此[[DataFrame]]
   *
   * @param weights weights for splits, will be normalized if they don't sum to 1.
   * @group dfops
   * @since 1.4.0
   */
  def randomSplit(weights: Array[Double]): Array[DataFrame] = {
    randomSplit(weights, Utils.random.nextLong)
  }

  /**
   * Randomly splits this [[DataFrame]] with the provided weights. Provided for the Python Api.
    * 使用提供的权重随机分割此[[DataFrame]]为Python Api提供
   *
   * @param weights weights for splits, will be normalized if they don't sum to 1.
   * @param seed Seed for sampling.
   * @group dfops
   */
  private[spark] def randomSplit(weights: List[Double], seed: Long): Array[DataFrame] = {
    randomSplit(weights.toArray, seed)
  }

  /**
   * (Scala-specific) Returns a new [[DataFrame]] where each row has been expanded to zero or more
   * rows by the provided function.  This is similar to a `LATERAL VIEW` in HiveQL. The columns of
   * the input row are implicitly joined with each row that is output by the function.
    * 特定于Scala）返回一个新的[[DataFrame]],其中每行已被提供的函数扩展为零或更多行,
    * 这类似于HiveQL中的“LATERAL VIEW”,输入行的列与函数输出的每一行隐式连接。
   *
   * The following example uses this function to count the number of books which contain
   * a given word:
   *
   * {{{
   *   case class Book(title: String, words: String)
   *   val df: RDD[Book]
   *
   *   case class Word(word: String)
   *   val allWords = df.explode('words) {
   *     case Row(words: String) => words.split(" ").map(Word(_))
   *   }
   *
   *   val bookCountPerWord = allWords.groupBy("word").agg(countDistinct("title"))
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  def explode[A <: Product : TypeTag](input: Column*)(f: Row => TraversableOnce[A]): DataFrame = {
    val schema = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]

    val elementTypes = schema.toAttributes.map { attr => (attr.dataType, attr.nullable) }
    val names = schema.toAttributes.map(_.name)
    val convert = CatalystTypeConverters.createToCatalystConverter(schema)

    val rowFunction =
      f.andThen(_.map(convert(_).asInstanceOf[InternalRow]))
    val generator = UserDefinedGenerator(elementTypes, rowFunction, input.map(_.expr))

    Generate(generator, join = true, outer = false,
      qualifier = None, names.map(UnresolvedAttribute(_)), logicalPlan)
  }

  /**
   * (Scala-specific) Returns a new [[DataFrame]] where a single column has been expanded to zero
   * or more rows by the provided function.  This is similar to a `LATERAL VIEW` in HiveQL. All
   * columns of the input row are implicitly joined with each value that is output by the function.
   *	简单的把字符串分割为数组,同时命新命名字的字段
   * {{{
   *   df.explode("words", "word"){words: String => words.split(" ")}
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  def explode[A, B : TypeTag](inputColumn: String, outputColumn: String)(f: A => TraversableOnce[B])
    : DataFrame = {
    val dataType = ScalaReflection.schemaFor[B].dataType
    //Nil是一个空的List,::向队列的头部追加数据,创造新的列表
    val attributes = AttributeReference(outputColumn, dataType)() :: Nil
    // TODO handle the metadata?
    val elementTypes = attributes.map { attr => (attr.dataType, attr.nullable) }
    val names = attributes.map(_.name)

    def rowFunction(row: Row): TraversableOnce[InternalRow] = {
      val convert = CatalystTypeConverters.createToCatalystConverter(dataType)
      f(row(0).asInstanceOf[A]).map(o => InternalRow(convert(o)))
    }
    val generator = UserDefinedGenerator(elementTypes, rowFunction, apply(inputColumn).expr :: Nil)

    Generate(generator, join = true, outer = false,
      qualifier = None, names.map(UnresolvedAttribute(_)), logicalPlan)
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Returns a new [[DataFrame]] by adding a column or replacing the existing column that has
   * the same name.
    * 通过添加列或替换具有相同名称的现有列来返回新的[[DataFrame]]
   * @group dfops
   * @since 1.3.0
   */
  def withColumn(colName: String, col: Column): DataFrame = {
    val resolver = sqlContext.analyzer.resolver
    val replaced = schema.exists(f => resolver(f.name, colName))
    if (replaced) {
      val colNames = schema.map { field =>
        val name = field.name
        if (resolver(name, colName)) col.as(colName) else Column(name)
      }
      select(colNames : _*)
    } else {
      select(Column("*"), col.as(colName))
    }
  }

  /**
   * Returns a new [[DataFrame]] with a column renamed.
   * This is a no-op if schema doesn't contain existingName.
    * 返回重命名列的新[[DataFrame]],
    * 如果架构不包含existingName,则这是一个无操作。
   * @group dfops
   * @since 1.3.0
   */
  def withColumnRenamed(existingName: String, newName: String): DataFrame = {
    val resolver = sqlContext.analyzer.resolver
    val shouldRename = schema.exists(f => resolver(f.name, existingName))
    if (shouldRename) {
      val colNames = schema.map { field =>
        val name = field.name
        if (resolver(name, existingName)) Column(name).as(newName) else Column(name)
      }
      select(colNames : _*)
    } else {
      this
    }
  }

  /**
   * Returns a new [[DataFrame]] with a column dropped.
    * 返回删除了列的新[[DataFrame]]
   * This is a no-op if schema doesn't contain column name.
   * @group dfops
   * @since 1.4.0
   */
  def drop(colName: String): DataFrame = {
    val resolver = sqlContext.analyzer.resolver
    val shouldDrop = schema.exists(f => resolver(f.name, colName))
    if (shouldDrop) {
      val colsAfterDrop = schema.filter { field =>
        val name = field.name
        !resolver(name, colName)
      }.map(f => Column(f.name))
      select(colsAfterDrop : _*)
    } else {
      this
    }
  }

  /**
   * Returns a new [[DataFrame]] with a column dropped.
    * 返回删除了列的新[[DataFrame]]
   * This version of drop accepts a Column rather than a name.
   * This is a no-op if the DataFrame doesn't have a column
   * with an equivalent expression.
   * @group dfops
   * @since 1.4.1
   */
  def drop(col: Column): DataFrame = {
    val attrs = this.logicalPlan.output
    val colsAfterDrop = attrs.filter { attr =>
      attr != col.expr
    }.map(attr => Column(attr))
    select(colsAfterDrop : _*)
  }

  /**
   * Returns a new [[DataFrame]] that contains only the unique rows from this [[DataFrame]].
    * 返回一个新的[[DataFrame]],它只包含此[[DataFrame]]中的唯一行
   * This is an alias for `distinct`.
   * @group dfops
   * @since 1.4.0
   */
  def dropDuplicates(): DataFrame = dropDuplicates(this.columns)

  /**
   * (Scala-specific) Returns a new [[DataFrame]] with duplicate rows removed, considering only
   * the subset of columns.
    * （特定于Scala）返回删除了重复行的新[[DataFrame]],仅考虑列的子集。
   *
   * @group dfops
   * @since 1.4.0
   */
  def dropDuplicates(colNames: Seq[String]): DataFrame = {
    val groupCols = colNames.map(resolve)
    val groupColExprIds = groupCols.map(_.exprId)
    val aggCols = logicalPlan.output.map { attr =>
      if (groupColExprIds.contains(attr.exprId)) {
        attr
      } else {
        Alias(First(attr), attr.name)()
      }
    }
    Aggregate(groupCols, aggCols, logicalPlan)
  }

  /**
   * Returns a new [[DataFrame]] with duplicate rows removed, considering only
   * the subset of columns.
    * 返回删除了重复行的新[[DataFrame]],仅考虑列的子集
   *
   * @group dfops
   * @since 1.4.0
   */
  def dropDuplicates(colNames: Array[String]): DataFrame = dropDuplicates(colNames.toSeq)

  /**
   * Computes statistics for numeric columns, including count, mean, stddev, min, and max.
   * If no columns are given, this function computes statistics for all numerical columns.
    *
   * 计算数字列的统计信息，包括count，mean，stddev，min和max.Computes统计数字列，
    * 包括count，mean，stddev，min和max。
    *
   * This function is meant for exploratory data analysis, as we make no guarantee about the
   * backward compatibility of the schema of the resulting [[DataFrame]]. If you want to
   * programmatically compute summary statistics, use the `agg` function instead.
   *
   * {{{
   *   df.describe("age", "height").show()
   *
   *   // output:
   *   // summary age   height
   *   // count   10.0  10.0
   *   // mean    53.3  178.05
   *   // stddev  11.6  15.7
   *   // min     18.0  163.0
   *   // max     92.0  192.0
   * }}}
   *
   * @group action
   * @since 1.3.1
   */
  @scala.annotation.varargs
  def describe(cols: String*): DataFrame = {

    // TODO: Add stddev as an expression, and remove it from here.
    def stddevExpr(expr: Expression): Expression =
      Sqrt(Subtract(Average(Multiply(expr, expr)), Multiply(Average(expr), Average(expr))))

    // The list of summary statistics to compute, in the form of expressions.
    //要以表达式的形式计算的摘要统计信息列表
    val statistics = List[(String, Expression => Expression)](
      "count" -> Count,
      "mean" -> Average,
      "stddev" -> stddevExpr,
      "min" -> Min,
      "max" -> Max)

    val outputCols = (if (cols.isEmpty) numericColumns.map(_.prettyString) else cols).toList

    val ret: Seq[Row] = if (outputCols.nonEmpty) {
      val aggExprs = statistics.flatMap { case (_, colToAgg) =>
        outputCols.map(c => Column(Cast(colToAgg(Column(c).expr), StringType)).as(c))
      }

      val row = agg(aggExprs.head, aggExprs.tail: _*).head().toSeq

      // Pivot the data so each summary is one row
      //透视数据,使每个摘要都是一行
      row.grouped(outputCols.size).toSeq.zip(statistics).map { case (aggregation, (statistic, _)) =>
        Row(statistic :: aggregation.toList: _*)
      }
    } else {
      // If there are no output columns, just output a single column that contains the stats.
      //如果没有输出列,则只输出包含统计信息的单个列
      statistics.map { case (name, _) => Row(name) }
    }

    // All columns are string type
    val schema = StructType(
      StructField("summary", StringType) :: outputCols.map(StructField(_, StringType))).toAttributes
    LocalRelation.fromExternalRows(schema, ret)
  }

  /**
   * Returns the first `n` rows.
    * 返回第一行`n`行。
   * @group action
   * @since 1.3.0
   */
  def head(n: Int): Array[Row] = limit(n).collect()

  /**
   * Returns the first row.
    * 返回第一行
   * @group action
   * @since 1.3.0
   */
  def head(): Row = head(1).head

  /**
   * Returns the first row. Alias for head().
    * 返回第一行,head()的别名
   * @group action
   * @since 1.3.0
   */
  def first(): Row = head()

  /**
   * Returns a new RDD by applying a function to all rows of this DataFrame.
    * 通过将函数应用于此DataFrame的所有行来返回新的RDD
   * @group rdd
   * @since 1.3.0
   */
  def map[R: ClassTag](f: Row => R): RDD[R] = rdd.map(f)

  /**
   * Returns a new RDD by first applying a function to all rows of this [[DataFrame]],
   * and then flattening the results.
    * 通过首先将函数应用于此[[DataFrame]]的所有行,然后展平结果,返回新的RDD。
   * @group rdd
   * @since 1.3.0
   */
  def flatMap[R: ClassTag](f: Row => TraversableOnce[R]): RDD[R] = rdd.flatMap(f)

  /**
   * Returns a new RDD by applying a function to each partition of this DataFrame.
    * 通过将函数应用于此DataFrame的每个分区来返回新的RDD
   * @group rdd
   * @since 1.3.0
   */
  def mapPartitions[R: ClassTag](f: Iterator[Row] => Iterator[R]): RDD[R] = {
    rdd.mapPartitions(f)
  }

  /**
   * Applies a function `f` to all rows.
    * 对所有行应用函数`f`
   * @group rdd
   * @since 1.3.0
   */
  def foreach(f: Row => Unit): Unit = withNewExecutionId {
    rdd.foreach(f)
  }

  /**
   * Applies a function f to each partition of this [[DataFrame]].
    * 将函数f应用于此[[DataFrame]]的每个分区
   * @group rdd
   * @since 1.3.0
   */
  def foreachPartition(f: Iterator[Row] => Unit): Unit = withNewExecutionId {
    rdd.foreachPartition(f)
  }

  /**
   * Returns the first `n` rows in the [[DataFrame]].
    * 返回[[DataFrame]]中的第一个`n`行
   * @group action
   * @since 1.3.0
   */
  def take(n: Int): Array[Row] = head(n)

  /**
   * Returns an array that contains all of [[Row]]s in this [[DataFrame]].
    * 返回一个包含此[[DataFrame]]中所有[[Row]]的数组
   * @group action
   * @since 1.3.0
   */
  def collect(): Array[Row] = withNewExecutionId {
    queryExecution.executedPlan.executeCollect()
  }

  /**
   * Returns a Java list that contains all of [[Row]]s in this [[DataFrame]].
    * 返回包含此[[DataFrame]]中所有[[Row]]的Java列表
   * @group action
   * @since 1.3.0
   */
  def collectAsList(): java.util.List[Row] = withNewExecutionId {
    java.util.Arrays.asList(rdd.collect() : _*)
  }

  /**
   * Returns the number of rows in the [[DataFrame]].
    * 返回[[DataFrame]]中的行数
   * @group action
   * @since 1.3.0
   */
  def count(): Long = groupBy().count().collect().head.getLong(0)

  /**
   * Returns a new [[DataFrame]] that has exactly `numPartitions` partitions.
    * 返回一个具有完全`numPartitions`分区的新[[DataFrame]]
   * @group rdd
   * @since 1.3.0
   */
  def repartition(numPartitions: Int): DataFrame = {
    Repartition(numPartitions, shuffle = true, logicalPlan)
  }

  /**
   * Returns a new [[DataFrame]] that has exactly `numPartitions` partitions.
    * 返回一个具有完全`numPartitions`分区的新[[DataFrame]]
   * Similar to coalesce defined on an [[RDD]], this operation results in a narrow dependency, e.g.
   * if you go from 1000 partitions to 100 partitions, there will not be a shuffle, instead each of
   * the 100 new partitions will claim 10 of the current partitions.
    * 类似于在[[RDD]]上定义的coalesce，此操作会导致较窄的依赖性,
    * 例如,如果从1000个分区到100个分区,则不会进行随机播放,而是100个新分区中的每个分区将声明10个当前分区。
   * @group rdd
   * @since 1.4.0
   */
  def coalesce(numPartitions: Int): DataFrame = {
    Repartition(numPartitions, shuffle = false, logicalPlan)
  }

  /**
   * Returns a new [[DataFrame]] that contains only the unique rows from this [[DataFrame]].
    * 返回一个新的[[DataFrame]]，它只包含此[[DataFrame]]中的唯一行
   * This is an alias for `dropDuplicates`.
   * @group dfops
   * @since 1.3.0
   */
  def distinct(): DataFrame = dropDuplicates()

  /**
   * @group basic
   * @since 1.3.0
   */
  def persist(): this.type = {
    sqlContext.cacheManager.cacheQuery(this)
    this
  }

  /**
   * @group basic
   * @since 1.3.0
   */
  def cache(): this.type = persist()

  /**
   * @group basic
   * @since 1.3.0
   */
  def persist(newLevel: StorageLevel): this.type = {
    sqlContext.cacheManager.cacheQuery(this, None, newLevel)
    this
  }

  /**
   * @group basic
   * @since 1.3.0
   */
  def unpersist(blocking: Boolean): this.type = {
    sqlContext.cacheManager.tryUncacheQuery(this, blocking)
    this
  }

  /**
   * @group basic
   * @since 1.3.0
   */
  def unpersist(): this.type = unpersist(blocking = false)

  /////////////////////////////////////////////////////////////////////////////
  // I/O
  /////////////////////////////////////////////////////////////////////////////

  /**
   * Represents the content of the [[DataFrame]] as an [[RDD]] of [[Row]]s. Note that the RDD is
   * memoized. Once called, it won't change even if you change any query planning related Spark SQL
   * configurations (e.g. `spark.sql.shuffle.partitions`).
   * @group rdd
   * @since 1.3.0
   */
  lazy val rdd: RDD[Row] = {
    // use a local variable to make sure the map closure doesn't capture the whole DataFrame
    //使用局部变量来确保映射闭包不捕获整个DataFrame
    val schema = this.schema
    queryExecution.toRdd.mapPartitions { rows =>
      val converter = CatalystTypeConverters.createToScalaConverter(schema)
      rows.map(converter(_).asInstanceOf[Row])
    }
  }

  /**
   * Returns the content of the [[DataFrame]] as a [[JavaRDD]] of [[Row]]s.
    * 将[[DataFrame]]的内容作为[[Row]]的[[JavaRDD]]返回
   * @group rdd
   * @since 1.3.0
   */
  def toJavaRDD: JavaRDD[Row] = rdd.toJavaRDD()

  /**
   * Returns the content of the [[DataFrame]] as a [[JavaRDD]] of [[Row]]s.
    * 将[[DataFrame]]的内容作为[[Row]]的[[JavaRDD]]返回
   * @group rdd
   * @since 1.3.0
   */
  def javaRDD: JavaRDD[Row] = toJavaRDD

  /**
   * Registers this [[DataFrame]] as a temporary table using the given name.  The lifetime of this
   * temporary table is tied to the [[SQLContext]] that was used to create this DataFrame.
    *
    * 使用给定名称将此[[DataFrame]]注册为临时表,
    * 此临时表的生命周期与用于创建此DataFrame的[[SQLContext]]相关联。
   *
   * @group basic
   * @since 1.3.0
   */
  def registerTempTable(tableName: String): Unit = {
    sqlContext.registerDataFrameAsTable(this, tableName)
  }

  /**
   * :: Experimental ::
   * Interface for saving the content of the [[DataFrame]] out into external storage.
    * 用于将[[DataFrame]]的内容保存到外部存储器的接口
   *
   * @group output
   * @since 1.4.0
   */
  @Experimental
  def write: DataFrameWriter = new DataFrameWriter(this)

  /**
   * Returns the content of the [[DataFrame]] as a RDD of JSON strings.
    * 返回[[DataFrame]]的内容作为JSON字符串的RDD
   * @group rdd
   * @since 1.3.0
   */
  def toJSON: RDD[String] = {
    val rowSchema = this.schema
    this.mapPartitions { iter =>
      val writer = new CharArrayWriter()
      // create the Generator without separator inserted between 2 records
      val gen = new JsonFactory().createGenerator(writer).setRootValueSeparator(null)

      new Iterator[String] {
        override def hasNext: Boolean = iter.hasNext
        override def next(): String = {
          JacksonGenerator(rowSchema, gen)(iter.next())
          gen.flush()

          val json = writer.toString
          if (hasNext) {
            writer.reset()
          } else {
            gen.close()
          }

          json
        }
      }
    }
  }

  /**
   * Returns a best-effort snapshot of the files that compose this DataFrame. This method simply
   * asks each constituent BaseRelation for its respective files and takes the union of all results.
   * Depending on the source relations, this may not find all input files. Duplicates are removed.
   */
  def inputFiles: Array[String] = {
    val files: Seq[String] = logicalPlan.collect {
      case LogicalRelation(fsBasedRelation: FileRelation, _) =>
        fsBasedRelation.inputFiles
      case fr: FileRelation =>
        fr.inputFiles
    }.flatten
    files.toSet.toArray
  }

  ////////////////////////////////////////////////////////////////////////////
  // for Python API
  ////////////////////////////////////////////////////////////////////////////

  /**
   * Converts a JavaRDD to a PythonRDD.
    * 将JavaRDD转换为PythonRDD
   */
  protected[sql] def javaToPython: JavaRDD[Array[Byte]] = {
    val structType = schema  // capture it for closure
    val rdd = queryExecution.toRdd.map(EvaluatePython.toJava(_, structType))
    EvaluatePython.javaToPython(rdd)
  }

  ////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////
  // Deprecated methods
  ////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////

  /**
   * @deprecated As of 1.3.0, replaced by `toDF()`.
   */
  @deprecated("use toDF", "1.3.0")
  def toSchemaRDD: DataFrame = this

  /**
   * Save this [[DataFrame]] to a JDBC database at `url` under the table name `table`.
   * This will run a `CREATE TABLE` and a bunch of `INSERT INTO` statements.
   * If you pass `true` for `allowExisting`, it will drop any table with the
   * given name; if you pass `false`, it will throw if the table already
   * exists.
   * @group output
   * @deprecated As of 1.340, replaced by `write().jdbc()`.
   */
  @deprecated("Use write.jdbc()", "1.4.0")
  def createJDBCTable(url: String, table: String, allowExisting: Boolean): Unit = {
    val w = if (allowExisting) write.mode(SaveMode.Overwrite) else write
    w.jdbc(url, table, new Properties)
  }

  /**
   * Save this [[DataFrame]] to a JDBC database at `url` under the table name `table`.
   * Assumes the table already exists and has a compatible schema.  If you
   * pass `true` for `overwrite`, it will `TRUNCATE` the table before
   * performing the `INSERT`s.
   *
   * The table must already exist on the database.  It must have a schema
   * that is compatible with the schema of this RDD; inserting the rows of
   * the RDD in order via the simple statement
   * `INSERT INTO table VALUES (?, ?, ..., ?)` should not fail.
   * @group output
   * @deprecated As of 1.4.0, replaced by `write().jdbc()`.
   */
  @deprecated("Use write.jdbc()", "1.4.0")
  def insertIntoJDBC(url: String, table: String, overwrite: Boolean): Unit = {
    val w = if (overwrite) write.mode(SaveMode.Overwrite) else write.mode(SaveMode.Append)
    w.jdbc(url, table, new Properties)
  }

  /**
   * Saves the contents of this [[DataFrame]] as a parquet file, preserving the schema.
   * Files that are written out using this method can be read back in as a [[DataFrame]]
   * using the `parquetFile` function in [[SQLContext]].
   * @group output
   * @deprecated As of 1.4.0, replaced by `write().parquet()`.
   */
  @deprecated("Use write.parquet(path)", "1.4.0")
  def saveAsParquetFile(path: String): Unit = {
    write.format("parquet").mode(SaveMode.ErrorIfExists).save(path)
  }

  /**
   * Creates a table from the the contents of this DataFrame.
   * It will use the default data source configured by spark.sql.sources.default.
   * This will fail if the table already exists.
   *
   * Note that this currently only works with DataFrames that are created from a HiveContext as
   * there is no notion of a persisted catalog in a standard SQL context.  Instead you can write
   * an RDD out to a parquet file, and then register that file as a table.  This "table" can then
   * be the target of an `insertInto`.
   *
   * When the DataFrame is created from a non-partitioned [[HadoopFsRelation]] with a single input
   * path, and the data source provider can be mapped to an existing Hive builtin SerDe (i.e. ORC
   * and Parquet), the table is persisted in a Hive compatible format, which means other systems
   * like Hive will be able to read this table. Otherwise, the table is persisted in a Spark SQL
   * specific format.
   *
   * @group output
   * @deprecated As of 1.4.0, replaced by `write().saveAsTable(tableName)`.
   */
  @deprecated("Use write.saveAsTable(tableName)", "1.4.0")
  def saveAsTable(tableName: String): Unit = {
    write.mode(SaveMode.ErrorIfExists).saveAsTable(tableName)
  }

  /**
   * Creates a table from the the contents of this DataFrame, using the default data source
   * configured by spark.sql.sources.default and [[SaveMode.ErrorIfExists]] as the save mode.
   *
   * Note that this currently only works with DataFrames that are created from a HiveContext as
   * there is no notion of a persisted catalog in a standard SQL context.  Instead you can write
   * an RDD out to a parquet file, and then register that file as a table.  This "table" can then
   * be the target of an `insertInto`.
   *
   * When the DataFrame is created from a non-partitioned [[HadoopFsRelation]] with a single input
   * path, and the data source provider can be mapped to an existing Hive builtin SerDe (i.e. ORC
   * and Parquet), the table is persisted in a Hive compatible format, which means other systems
   * like Hive will be able to read this table. Otherwise, the table is persisted in a Spark SQL
   * specific format.
   *
   * @group output
   * @deprecated As of 1.4.0, replaced by `write().mode(mode).saveAsTable(tableName)`.
   */
  @deprecated("Use write.mode(mode).saveAsTable(tableName)", "1.4.0")
  def saveAsTable(tableName: String, mode: SaveMode): Unit = {
    write.mode(mode).saveAsTable(tableName)
  }

  /**
   * Creates a table at the given path from the the contents of this DataFrame
   * based on a given data source and a set of options,
   * using [[SaveMode.ErrorIfExists]] as the save mode.
   *
   * Note that this currently only works with DataFrames that are created from a HiveContext as
   * there is no notion of a persisted catalog in a standard SQL context.  Instead you can write
   * an RDD out to a parquet file, and then register that file as a table.  This "table" can then
   * be the target of an `insertInto`.
   *
   * When the DataFrame is created from a non-partitioned [[HadoopFsRelation]] with a single input
   * path, and the data source provider can be mapped to an existing Hive builtin SerDe (i.e. ORC
   * and Parquet), the table is persisted in a Hive compatible format, which means other systems
   * like Hive will be able to read this table. Otherwise, the table is persisted in a Spark SQL
   * specific format.
   *
   * @group output
   * @deprecated As of 1.4.0, replaced by `write().format(source).saveAsTable(tableName)`.
   */
  @deprecated("Use write.format(source).saveAsTable(tableName)", "1.4.0")
  def saveAsTable(tableName: String, source: String): Unit = {
    write.format(source).saveAsTable(tableName)
  }

  /**
   * :: Experimental ::
   * Creates a table at the given path from the the contents of this DataFrame
   * based on a given data source, [[SaveMode]] specified by mode, and a set of options.
   *
   * Note that this currently only works with DataFrames that are created from a HiveContext as
   * there is no notion of a persisted catalog in a standard SQL context.  Instead you can write
   * an RDD out to a parquet file, and then register that file as a table.  This "table" can then
   * be the target of an `insertInto`.
   *
   * When the DataFrame is created from a non-partitioned [[HadoopFsRelation]] with a single input
   * path, and the data source provider can be mapped to an existing Hive builtin SerDe (i.e. ORC
   * and Parquet), the table is persisted in a Hive compatible format, which means other systems
   * like Hive will be able to read this table. Otherwise, the table is persisted in a Spark SQL
   * specific format.
   *
   * @group output
   * @deprecated As of 1.4.0, replaced by `write().mode(mode).saveAsTable(tableName)`.
   */
  @deprecated("Use write.format(source).mode(mode).saveAsTable(tableName)", "1.4.0")
  def saveAsTable(tableName: String, source: String, mode: SaveMode): Unit = {
    write.format(source).mode(mode).saveAsTable(tableName)
  }

  /**
   * Creates a table at the given path from the the contents of this DataFrame
   * based on a given data source, [[SaveMode]] specified by mode, and a set of options.
   *
   * Note that this currently only works with DataFrames that are created from a HiveContext as
   * there is no notion of a persisted catalog in a standard SQL context.  Instead you can write
   * an RDD out to a parquet file, and then register that file as a table.  This "table" can then
   * be the target of an `insertInto`.
   *
   * When the DataFrame is created from a non-partitioned [[HadoopFsRelation]] with a single input
   * path, and the data source provider can be mapped to an existing Hive builtin SerDe (i.e. ORC
   * and Parquet), the table is persisted in a Hive compatible format, which means other systems
   * like Hive will be able to read this table. Otherwise, the table is persisted in a Spark SQL
   * specific format.
   *
   * @group output
   * @deprecated As of 1.4.0, replaced by
   *            `write().format(source).mode(mode).options(options).saveAsTable(tableName)`.
   */
  @deprecated("Use write.format(source).mode(mode).options(options).saveAsTable(tableName)",
    "1.4.0")
  def saveAsTable(
      tableName: String,
      source: String,
      mode: SaveMode,
      options: java.util.Map[String, String]): Unit = {
    write.format(source).mode(mode).options(options).saveAsTable(tableName)
  }

  /**
   * (Scala-specific)
   * Creates a table from the the contents of this DataFrame based on a given data source,
   * [[SaveMode]] specified by mode, and a set of options.
   *
   * Note that this currently only works with DataFrames that are created from a HiveContext as
   * there is no notion of a persisted catalog in a standard SQL context.  Instead you can write
   * an RDD out to a parquet file, and then register that file as a table.  This "table" can then
   * be the target of an `insertInto`.
   *
   * When the DataFrame is created from a non-partitioned [[HadoopFsRelation]] with a single input
   * path, and the data source provider can be mapped to an existing Hive builtin SerDe (i.e. ORC
   * and Parquet), the table is persisted in a Hive compatible format, which means other systems
   * like Hive will be able to read this table. Otherwise, the table is persisted in a Spark SQL
   * specific format.
   *
   * @group output
   * @deprecated As of 1.4.0, replaced by
   *            `write().format(source).mode(mode).options(options).saveAsTable(tableName)`.
   */
  @deprecated("Use write.format(source).mode(mode).options(options).saveAsTable(tableName)",
    "1.4.0")
  def saveAsTable(
      tableName: String,
      source: String,
      mode: SaveMode,
      options: Map[String, String]): Unit = {
    write.format(source).mode(mode).options(options).saveAsTable(tableName)
  }

  /**
   * Saves the contents of this DataFrame to the given path,
   * using the default data source configured by spark.sql.sources.default and
   * [[SaveMode.ErrorIfExists]] as the save mode.
   * @group output
   * @deprecated As of 1.4.0, replaced by `write().save(path)`.
   */
  @deprecated("Use write.save(path)", "1.4.0")
  def save(path: String): Unit = {
    write.save(path)
  }

  /**
   * Saves the contents of this DataFrame to the given path and [[SaveMode]] specified by mode,
   * using the default data source configured by spark.sql.sources.default.
   * @group output
   * @deprecated As of 1.4.0, replaced by `write().mode(mode).save(path)`.
   */
  @deprecated("Use write.mode(mode).save(path)", "1.4.0")
  def save(path: String, mode: SaveMode): Unit = {
    write.mode(mode).save(path)
  }

  /**
   * Saves the contents of this DataFrame to the given path based on the given data source,
   * using [[SaveMode.ErrorIfExists]] as the save mode.
   * @group output
   * @deprecated As of 1.4.0, replaced by `write().format(source).save(path)`.
   */
  @deprecated("Use write.format(source).save(path)", "1.4.0")
  def save(path: String, source: String): Unit = {
    write.format(source).save(path)
  }

  /**
   * Saves the contents of this DataFrame to the given path based on the given data source and
   * [[SaveMode]] specified by mode.
   * @group output
   * @deprecated As of 1.4.0, replaced by `write().format(source).mode(mode).save(path)`.
   */
  @deprecated("Use write.format(source).mode(mode).save(path)", "1.4.0")
  def save(path: String, source: String, mode: SaveMode): Unit = {
    write.format(source).mode(mode).save(path)
  }

  /**
   * Saves the contents of this DataFrame based on the given data source,
   * [[SaveMode]] specified by mode, and a set of options.
   * @group output
   * @deprecated As of 1.4.0, replaced by
   *            `write().format(source).mode(mode).options(options).save(path)`.
   */
  @deprecated("Use write.format(source).mode(mode).options(options).save()", "1.4.0")
  def save(
      source: String,
      mode: SaveMode,
      options: java.util.Map[String, String]): Unit = {
    write.format(source).mode(mode).options(options).save()
  }

  /**
   * (Scala-specific)
   * Saves the contents of this DataFrame based on the given data source,
   * [[SaveMode]] specified by mode, and a set of options
   * @group output
   * @deprecated As of 1.4.0, replaced by
   *            `write().format(source).mode(mode).options(options).save(path)`.
   */
  @deprecated("Use write.format(source).mode(mode).options(options).save()", "1.4.0")
  def save(
      source: String,
      mode: SaveMode,
      options: Map[String, String]): Unit = {
    write.format(source).mode(mode).options(options).save()
  }


  /**
   * Adds the rows from this RDD to the specified table, optionally overwriting the existing data.
   * @group output
   * @deprecated As of 1.4.0, replaced by
   *            `write().mode(SaveMode.Append|SaveMode.Overwrite).saveAsTable(tableName)`.
   */
  @deprecated("Use write.mode(SaveMode.Append|SaveMode.Overwrite).saveAsTable(tableName)", "1.4.0")
  def insertInto(tableName: String, overwrite: Boolean): Unit = {
    write.mode(if (overwrite) SaveMode.Overwrite else SaveMode.Append).insertInto(tableName)
  }

  /**
   * Adds the rows from this RDD to the specified table.
   * Throws an exception if the table already exists.
   * @group output
   * @deprecated As of 1.4.0, replaced by
   *            `write().mode(SaveMode.Append).saveAsTable(tableName)`.
   */
  @deprecated("Use write.mode(SaveMode.Append).saveAsTable(tableName)", "1.4.0")
  def insertInto(tableName: String): Unit = {
    write.mode(SaveMode.Append).insertInto(tableName)
  }

  /**
   * Wrap a DataFrame action to track all Spark jobs in the body so that we can connect them with
   * an execution.
   */
  private[sql] def withNewExecutionId[T](body: => T): T = {
    SQLExecution.withNewExecutionId(sqlContext, queryExecution)(body)
  }

  ////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////
  // End of deprecated methods
  ////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////

}
