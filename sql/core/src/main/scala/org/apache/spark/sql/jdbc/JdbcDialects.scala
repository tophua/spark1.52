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

package org.apache.spark.sql.jdbc

import java.sql.Types

import org.apache.spark.sql.types._
import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * A database type definition coupled with the jdbc type needed to send null
 * values to the database.
 * @param databaseTypeDefinition The database type definition
 * @param jdbcNullType The jdbc type (as defined in java.sql.Types) used to
 *                     send a null value to the database.
 */
@DeveloperApi
case class JdbcType(databaseTypeDefinition : String, jdbcNullType : Int)

/**
 * :: DeveloperApi ::
 * Encapsulates everything (extensions, workarounds, quirks) to handle the
 * SQL dialect of a certain database or jdbc driver.
  * 封装所有内容(扩展,变通方法,怪癖)来处理某个数据库或jdbc驱动程序的SQL方言。
 * Lots of databases define types that aren't explicitly supported
 * by the JDBC spec.  Some JDBC drivers also report inaccurate
 * information---for instance, BIT(n>1) being reported as a BIT type is quite
 * common, even though BIT in JDBC is meant for single-bit values.  Also, there
 * does not appear to be a standard name for an unbounded string or binary
 * type; we use BLOB and CLOB by default but override with database-specific
 * alternatives when these are absent or do not behave correctly.
  * 许多数据库定义了JDBC规范未明确支持的类型,一些JDBC驱动程序也报告不准确的信息---
  * 例如,BIT（n> 1）被报告为BIT类型是很常见的，即使JDBC中的BIT用于单比特值,
  * 此外,似乎没有无界字符串或二进制类型的标准名称;
  * 我们默认使用BLOB和CLOB,但是当这些替代品不存在或行为不正确时覆盖它们。
 *
 * Currently, the only thing done by the dialect is type mapping.
 * `getCatalystType` is used when reading from a JDBC table and `getJDBCType`
 * is used when writing to a JDBC table.  If `getCatalystType` returns `null`,
 * the default type handling is used for the given JDBC type.  Similarly,
 * if `getJDBCType` returns `(null, None)`, the default type handling is used
 * for the given Catalyst type.
 */
@DeveloperApi
abstract class JdbcDialect {
  /**
   * Check if this dialect instance can handle a certain jdbc url.
    * 检查此方言实例是否可以处理某个jdbc url
   * @param url the jdbc url.
   * @return True if the dialect can be applied on the given jdbc url.
   * @throws NullPointerException if the url is null.
   */
  def canHandle(url : String): Boolean

  /**
   * Get the custom datatype mapping for the given jdbc meta information.
    * 获取给定jdbc元信息的自定义数据类型映射
   * @param sqlType The sql type (see java.sql.Types)
   * @param typeName The sql type name (e.g. "BIGINT UNSIGNED")
   * @param size The size of the type.
   * @param md Result metadata associated with this type.
   * @return The actual DataType (subclasses of [[org.apache.spark.sql.types.DataType]])
   *         or null if the default type mapping should be used.
   */
  def getCatalystType(
    sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = None

  /**
   * Retrieve the jdbc / sql type for a given datatype.
    * 检索给定数据类型的jdbc/sql类型
   * @param dt The datatype (e.g. [[org.apache.spark.sql.types.StringType]])
   * @return The new JdbcType if there is an override for this DataType
   */
  def getJDBCType(dt: DataType): Option[JdbcType] = None

  /**
   * Quotes the identifier. This is used to put quotes around the identifier in case the column
   * name is a reserved keyword, or in case it contains characters that require quotes (e.g. space).
    *
    * 引用标识符,这用于在标识符引用标识符的情况下在标识符周围加上引号,这用于在列的情况下在标识符周围加上引号
   */
  def quoteIdentifier(colName: String): String = {
    s""""$colName""""
  }
}

/**
 * :: DeveloperApi ::
 * Registry of dialects that apply to every new jdbc [[org.apache.spark.sql.DataFrame]].
  * 适用于每个新jdbc [[org.apache.spark.sql.DataFrame]]的方言注册表
 *
 * If multiple matching dialects are registered then all matching ones will be
 * tried in reverse order. A user-added dialect will thus be applied first,
 * overwriting the defaults.
  * 如果注册了多个匹配的方言,则将以相反的顺序尝试所有匹配的方言,
  * 因此,将首先应用用户添加的方言,覆盖默认值。
 *
 * Note that all new dialects are applied to new jdbc DataFrames only. Make
 * sure to register your dialects first.
 */
@DeveloperApi
object JdbcDialects {

  private var dialects = List[JdbcDialect]()

  /**
   * Register a dialect for use on all new matching jdbc [[org.apache.spark.sql.DataFrame]].
   * Readding an existing dialect will cause a move-to-front.
    * 注册方言以用于所有新匹配的jdbc [[org.apache.spark.sql.DataFrame]],读取现有的方言将导致前进
   * @param dialect The new dialect.
   */
  def registerDialect(dialect: JdbcDialect) : Unit = {
    dialects = dialect :: dialects.filterNot(_ == dialect)
  }

  /**
   * Unregister a dialect. Does nothing if the dialect is not registered.
    * 取消注册方言,如果没有注册方言,什么都不做
   * @param dialect The jdbc dialect.
   */
  def unregisterDialect(dialect : JdbcDialect) : Unit = {
    dialects = dialects.filterNot(_ == dialect)
  }

  registerDialect(MySQLDialect)
  registerDialect(PostgresDialect)
  registerDialect(OracleDialect)

  /**
   * Fetch the JdbcDialect class corresponding to a given database url.
    * 获取与给定数据库URL对应的JdbcDialect类
   */
  private[sql] def get(url: String): JdbcDialect = {
    val matchingDialects = dialects.filter(_.canHandle(url))
    matchingDialects.length match {
      case 0 => NoopDialect
      case 1 => matchingDialects.head
      case _ => new AggregatedDialect(matchingDialects)
    }
  }
}

/**
 * :: DeveloperApi ::
 * AggregatedDialect can unify multiple dialects into one virtual Dialect.
  * AggregatedDialect可以将多个方言统一为一个虚拟方言
 * Dialects are tried in order, and the first dialect that does not return a
 * neutral element will will.
  * 方言是按顺序尝试的,第一种不返回中性元素的方言将会
 * @param dialects List of dialects.
 */
@DeveloperApi
class AggregatedDialect(dialects: List[JdbcDialect]) extends JdbcDialect {

  require(dialects.nonEmpty)

  override def canHandle(url : String): Boolean =
    dialects.map(_.canHandle(url)).reduce(_ && _)

  override def getCatalystType(
      sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    dialects.flatMap(_.getCatalystType(sqlType, typeName, size, md)).headOption
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = {
    dialects.flatMap(_.getJDBCType(dt)).headOption
  }
}

/**
 * :: DeveloperApi ::
 * NOOP dialect object, always returning the neutral element.
 */
@DeveloperApi
case object NoopDialect extends JdbcDialect {
  override def canHandle(url : String): Boolean = true
}

/**
 * :: DeveloperApi ::
 * Default postgres dialect, mapping bit/cidr/inet on read and string/binary/boolean on write.
  * 默认的postgres方言，在读取时映射bit / cidr / inet，在写入时映射string / binary / boolean
 */
@DeveloperApi
case object PostgresDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean = url.startsWith("jdbc:postgresql")
  override def getCatalystType(
      sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    if (sqlType == Types.BIT && typeName.equals("bit") && size != 1) {
      Some(BinaryType)
    } else if (sqlType == Types.OTHER && typeName.equals("cidr")) {
      Some(StringType)
    } else if (sqlType == Types.OTHER && typeName.equals("inet")) {
      Some(StringType)
    } else None
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case StringType => Some(JdbcType("TEXT", java.sql.Types.CHAR))
    case BinaryType => Some(JdbcType("BYTEA", java.sql.Types.BINARY))
    case BooleanType => Some(JdbcType("BOOLEAN", java.sql.Types.BOOLEAN))
    case _ => None
  }
}

/**
 * :: DeveloperApi ::
 * Default mysql dialect to read bit/bitsets correctly.
 */
@DeveloperApi
case object MySQLDialect extends JdbcDialect {
  override def canHandle(url : String): Boolean = url.startsWith("jdbc:mysql")
  override def getCatalystType(
      sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    if (sqlType == Types.VARBINARY && typeName.equals("BIT") && size != 1) {
      // This could instead be a BinaryType if we'd rather return bit-vectors of up to 64 bits as
      // byte arrays instead of longs.
      md.putLong("binarylong", 1)
      Some(LongType)
    } else if (sqlType == Types.BIT && typeName.equals("TINYINT")) {
      Some(BooleanType)
    } else None
  }

  override def quoteIdentifier(colName: String): String = {
    s"`$colName`"
  }
}

/**
 * :: DeveloperApi ::
 * Default Oracle dialect, mapping a nonspecific numeric type to a general decimal type.
  * 默认Oracle方言，将非特定数字类型映射到一般十进制类型
 */
@DeveloperApi
case object OracleDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean = url.startsWith("jdbc:oracle")
  override def getCatalystType(
      sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    // Handle NUMBER fields that have no precision/scale in special way
    // because JDBC ResultSetMetaData converts this to 0 precision and -127 scale
    // For more details, please see
    // https://github.com/apache/spark/pull/8780#issuecomment-145598968
    // and
    // https://github.com/apache/spark/pull/8780#issuecomment-144541760
    if (sqlType == Types.NUMERIC && size == 0) {
      // This is sub-optimal as we have to pick a precision/scale in advance whereas the data
      // in Oracle is allowed to have different precision/scale for each value.
      Some(DecimalType(38, 10))
    } else {
      None
    }
  }
}
