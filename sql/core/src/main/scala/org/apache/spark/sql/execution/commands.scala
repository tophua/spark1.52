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

package org.apache.spark.sql.execution

import java.util.NoSuchElementException

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{InternalRow, CatalystTypeConverters}
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions.{ExpressionDescription, Expression, Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLConf, SQLContext}

/**
 * A logical command that is executed for its side-effects.  `RunnableCommand`s are
 * wrapped in `ExecutedCommand` during execution.
  * 为其副作用执行的逻辑命令,执行期间,`RunnableCommand'被包装在`ExecutedCommand`中。
 */
private[sql] trait RunnableCommand extends LogicalPlan with logical.Command {
  override def output: Seq[Attribute] = Seq.empty
  override def children: Seq[LogicalPlan] = Seq.empty
  def run(sqlContext: SQLContext): Seq[Row]
}

/**
 * A physical operator that executes the run method of a `RunnableCommand` and
 * saves the result to prevent multiple executions.
  * 一个物理运算符,它执行`RunnableCommand`的run方法并保存结果以防止多次执行
 */
private[sql] case class ExecutedCommand(cmd: RunnableCommand) extends SparkPlan {
  /**
   * A concrete command should override this lazy field to wrap up any side effects caused by the
   * command or any other computation that should be evaluated exactly once. The value of this field
   * can be used as the contents of the corresponding RDD generated from the physical plan of this
   * command.
    * 具体命令应该覆盖此惰性字段,以包装由命令或任何其他应该仅计算一次的计算引起的任何副作用,
    * 此字段的值可用作从此命令的物理计划生成的相应RDD的内容。
   *
   * The `execute()` method of all the physical command classes should reference `sideEffectResult`
   * so that the command can be executed eagerly right after the command query is created.
   */
  protected[sql] lazy val sideEffectResult: Seq[Row] = cmd.run(sqlContext)

  override def output: Seq[Attribute] = cmd.output

  override def children: Seq[SparkPlan] = Nil

  override def executeCollect(): Array[Row] = sideEffectResult.toArray

  override def executeTake(limit: Int): Array[Row] = sideEffectResult.take(limit).toArray

  protected override def doExecute(): RDD[InternalRow] = {
    val convert = CatalystTypeConverters.createToCatalystConverter(schema)
    val converted = sideEffectResult.map(convert(_).asInstanceOf[InternalRow])
    sqlContext.sparkContext.parallelize(converted, 1)
  }

  override def argString: String = cmd.toString
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class SetCommand(kv: Option[(String, Option[String])]) extends RunnableCommand with Logging {

  private def keyValueOutput: Seq[Attribute] = {
    val schema = StructType(
      //Nil是一个空的List,::向队列的头部追加数据,创造新的列表
      StructField("key", StringType, false) ::
        StructField("value", StringType, false) :: Nil)
    schema.toAttributes
  }

  private val (_output, runFunc): (Seq[Attribute], SQLContext => Seq[Row]) = kv match {
    // Configures the deprecated "mapred.reduce.tasks" property.
      //配置已弃用的“mapred.reduce.tasks”属性
    case Some((SQLConf.Deprecated.MAPRED_REDUCE_TASKS, Some(value))) =>
      val runFunc = (sqlContext: SQLContext) => {
        logWarning(
          s"Property ${SQLConf.Deprecated.MAPRED_REDUCE_TASKS} is deprecated, " +
            s"automatically converted to ${SQLConf.SHUFFLE_PARTITIONS.key} instead.")
        if (value.toInt < 1) {
          val msg =
            s"Setting negative ${SQLConf.Deprecated.MAPRED_REDUCE_TASKS} for automatically " +
              "determining the number of reducers is not supported."
          throw new IllegalArgumentException(msg)
        } else {
          sqlContext.setConf(SQLConf.SHUFFLE_PARTITIONS.key, value)
          Seq(Row(SQLConf.SHUFFLE_PARTITIONS.key, value))
        }
      }
      (keyValueOutput, runFunc)

    // Configures a single property.
      //配置单个属性
    case Some((key, Some(value))) =>
      val runFunc = (sqlContext: SQLContext) => {
        sqlContext.setConf(key, value)
        Seq(Row(key, value))
      }
      (keyValueOutput, runFunc)

    // (In Hive, "SET" returns all changed properties while "SET -v" returns all properties.)
      //在Hive中，“SET”返回所有已更改的属性，而“SET -v”返回所有属性)
    // Queries all key-value pairs that are set in the SQLConf of the sqlContext.
      //查询在sqlContext的SQLConf中设置的所有键值对
    case None =>
      val runFunc = (sqlContext: SQLContext) => {
        sqlContext.getAllConfs.map { case (k, v) => Row(k, v) }.toSeq
      }
      (keyValueOutput, runFunc)

    // Queries all properties along with their default values and docs that are defined in the
    // SQLConf of the sqlContext.
      //查询所有属性及其在sqlContext的SQLConf中定义的默认值和文档
    case Some(("-v", None)) =>
      val runFunc = (sqlContext: SQLContext) => {
        sqlContext.conf.getAllDefinedConfs.map { case (key, defaultValue, doc) =>
          Row(key, defaultValue, doc)
        }
      }
      val schema = StructType(
        //Nil是一个空的List,::向队列的头部追加数据,创造新的列表
        StructField("key", StringType, false) ::
          StructField("default", StringType, false) ::
          StructField("meaning", StringType, false) :: Nil)
      (schema.toAttributes, runFunc)

    // Queries the deprecated "mapred.reduce.tasks" property.
      //查询已弃用的“mapred.reduce.tasks”属性
    case Some((SQLConf.Deprecated.MAPRED_REDUCE_TASKS, None)) =>
      val runFunc = (sqlContext: SQLContext) => {
        logWarning(
          s"Property ${SQLConf.Deprecated.MAPRED_REDUCE_TASKS} is deprecated, " +
            s"showing ${SQLConf.SHUFFLE_PARTITIONS.key} instead.")
        Seq(Row(SQLConf.SHUFFLE_PARTITIONS.key, sqlContext.conf.numShufflePartitions.toString))
      }
      (keyValueOutput, runFunc)

    // Queries a single property.查询单个属性
    case Some((key, None)) =>
      val runFunc = (sqlContext: SQLContext) => {
        val value =
          try {
            sqlContext.getConf(key)
          } catch {
            case _: NoSuchElementException => "<undefined>"
          }
        Seq(Row(key, value))
      }
      (keyValueOutput, runFunc)
  }

  override val output: Seq[Attribute] = _output

  override def run(sqlContext: SQLContext): Seq[Row] = runFunc(sqlContext)

}

/**
 * An explain command for users to see how a command will be executed.
 * 解释命令用户查看命令将如何执行的命令
 * Note that this command takes in a logical plan, runs the optimizer on the logical plan
 * 请注意,这个命令需要一个逻辑计划,运行逻辑计划的优化程序
 * (but do NOT actually execute it).
 *
 * :: DeveloperApi ::
 */
@DeveloperApi
case class ExplainCommand(
    logicalPlan: LogicalPlan,
    override val output: Seq[Attribute] =
      Seq(AttributeReference("plan", StringType, nullable = false)()),
    extended: Boolean = false)
  extends RunnableCommand {

  // Run through the optimizer to generate the physical plan.
  //运行优化程序以生成物理计划
  override def run(sqlContext: SQLContext): Seq[Row] = try {
    // TODO in Hive, the "extended" ExplainCommand prints the AST as well, and detailed properties.
    val queryExecution = sqlContext.executePlan(logicalPlan)
    val outputString = if (extended) queryExecution.toString else queryExecution.simpleString

    outputString.split("\n").map(Row(_))
  } catch { case cause: TreeNodeException[_] =>
    ("Error occurred during query planning: \n" + cause.getMessage).split("\n").map(Row(_))
  }
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class CacheTableCommand(
    tableName: String,
    plan: Option[LogicalPlan],
    isLazy: Boolean)
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    plan.foreach { logicalPlan =>
      sqlContext.registerDataFrameAsTable(DataFrame(sqlContext, logicalPlan), tableName)
    }
    sqlContext.cacheTable(tableName)

    if (!isLazy) {
      // Performs eager caching 执行急切缓存
      sqlContext.table(tableName).count()
    }

    Seq.empty[Row]
  }

  override def output: Seq[Attribute] = Seq.empty
}


/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class UncacheTableCommand(tableName: String) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    sqlContext.table(tableName).unpersist(blocking = false)
    Seq.empty[Row]
  }

  override def output: Seq[Attribute] = Seq.empty
}

/**
 * :: DeveloperApi ::
 * Clear all cached data from the in-memory cache.
 * 清除内存缓存中的所有缓存数据
 */
@DeveloperApi
case object ClearCacheCommand extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    sqlContext.clearCache()
    Seq.empty[Row]
  }

  override def output: Seq[Attribute] = Seq.empty
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class DescribeCommand(
    child: SparkPlan,
    override val output: Seq[Attribute],
    isExtended: Boolean)
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    child.schema.fields.map { field =>
      val cmtKey = "comment"
      val comment = if (field.metadata.contains(cmtKey)) field.metadata.getString(cmtKey) else ""
      Row(field.name, field.dataType.simpleString, comment)
    }
  }
}

/**
 * A command for users to get tables in the given database.
 * 一个用户在给定数据库中获取表的命令
 * If a databaseName is not given, the current database will be used.
 * 如果没有给出一个数据库,将使用当前数据库,
 * The syntax of using this command in SQL is:
 * {{{
 *    SHOW TABLES [IN databaseName]
 * }}}
 * :: DeveloperApi ::
 */
@DeveloperApi
case class ShowTablesCommand(databaseName: Option[String]) extends RunnableCommand {

  // The result of SHOW TABLES has two columns, tableName and isTemporary.
  //SHOW TABLES的结果有两列,tableName和isTemporary
  override val output: Seq[Attribute] = {
    val schema = StructType(
      StructField("tableName", StringType, false) ::
      StructField("isTemporary", BooleanType, false) :: Nil)

    schema.toAttributes
  }

  override def run(sqlContext: SQLContext): Seq[Row] = {
    // Since we need to return a Seq of rows, we will call getTables directly
    // instead of calling tables in sqlContext.
    //由于我们需要返回一行Seq,我们将直接调用getTables而不是在sqlContext中调用表
    val rows = sqlContext.catalog.getTables(databaseName).map {
      case (tableName, isTemporary) => Row(tableName, isTemporary)
    }

    rows
  }
}

/**
 * A command for users to list all of the registered functions.
 * 命令为用户列出所有的注册函数
 * The syntax of using this command in SQL is:
 * {{{
 *    SHOW FUNCTIONS
 * }}}
 * TODO currently we are simply ignore the db
 */
case class ShowFunctions(db: Option[String], pattern: Option[String]) extends RunnableCommand {
  override val output: Seq[Attribute] = {
    val schema = StructType(
      //Nil是一个空的List,::向队列的头部追加数据,创造新的列表
      StructField("function", StringType, nullable = false) :: Nil)

    schema.toAttributes
  }

  override def run(sqlContext: SQLContext): Seq[Row] = pattern match {
    case Some(p) =>
      try {
        val regex = java.util.regex.Pattern.compile(p)
        sqlContext.functionRegistry.listFunction().filter(regex.matcher(_).matches()).map(Row(_))
      } catch {
        // probably will failed in the regex that user provided, then returns empty row.
        //可能会在用户提供的正则表达式中失败,然后返回空行
        case _: Throwable => Seq.empty[Row]
      }
    case None =>
      sqlContext.functionRegistry.listFunction().map(Row(_))
  }
}

/**
 * A command for users to get the usage of a registered function.
 * 命令用户获取注册函数的用法
 * The syntax of using this command in SQL is
 * {{{
 *   DESCRIBE FUNCTION [EXTENDED] upper;
 * }}}
 */
case class DescribeFunction(
    functionName: String,
    isExtended: Boolean) extends RunnableCommand {

  override val output: Seq[Attribute] = {
    val schema = StructType(
      //Nil是一个空的List,::向队列的头部追加数据,创造新的列表
      StructField("function_desc", StringType, nullable = false) :: Nil)

    schema.toAttributes
  }

  private def replaceFunctionName(usage: String, functionName: String): String = {
    if (usage == null) {
      "To be added."
    } else {
      usage.replaceAll("_FUNC_", functionName)
    }
  }

  override def run(sqlContext: SQLContext): Seq[Row] = {
    sqlContext.functionRegistry.lookupFunction(functionName) match {
      case Some(info) =>
        val result =
          Row(s"Function: ${info.getName}") ::
          Row(s"Class: ${info.getClassName}") ::
            //Nil是一个空的List,::向队列的头部追加数据,创造新的列表
          Row(s"Usage: ${replaceFunctionName(info.getUsage(), info.getName)}") :: Nil

        if (isExtended) {
          result :+ Row(s"Extended Usage:\n${replaceFunctionName(info.getExtended, info.getName)}")
        } else {
          result
        }

      case None => Seq(Row(s"Function: $functionName is not found."))
    }
  }
}
