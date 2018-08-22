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

package org.apache.spark.sql.execution.datasources.jdbc

import java.sql.{Connection, PreparedStatement}
import java.util.Properties

import scala.util.Try

import org.apache.spark.Logging
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

/**
 * Util functions for JDBC tables.
  * DBC表的Util函数
 */
object JdbcUtils extends Logging {

  /**
   * Establishes a JDBC connection.
    * 建立JDBC连接
   */
  def createConnection(url: String, connectionProperties: Properties): Connection = {
    JDBCRDD.getConnector(connectionProperties.getProperty("driver"), url, connectionProperties)()
  }

  /**
   * Returns true if the table already exists in the JDBC database.
    * 如果JDBC数据库中已存在该表,则返回true
   */
  def tableExists(conn: Connection, table: String): Boolean = {
    // Somewhat hacky, but there isn't a good way to identify whether a table exists for all
    // SQL database systems, considering "table" could also include the database name.
    //有点hacky,但没有一种很好的方法来确定是否存在所有SQL数据库系统的表,考虑到“table”也可以包括数据库名称。
    Try(conn.prepareStatement(s"SELECT 1 FROM $table LIMIT 1").executeQuery().next()).isSuccess
  }

  /**
   * Drops a table from the JDBC database.
    * 从JDBC数据库中删除表
   */
  def dropTable(conn: Connection, table: String): Unit = {
    conn.prepareStatement(s"DROP TABLE $table").executeUpdate()
  }

  /**
   * Returns a PreparedStatement that inserts a row into table via conn.
    * 返回一个PreparedStatement,它通过conn将一行插入表中
   */
  def insertStatement(conn: Connection, table: String, rddSchema: StructType): PreparedStatement = {
    val sql = new StringBuilder(s"INSERT INTO $table VALUES (")
    var fieldsLeft = rddSchema.fields.length
    while (fieldsLeft > 0) {
      sql.append("?")
      if (fieldsLeft > 1) sql.append(", ") else sql.append(")")
      fieldsLeft = fieldsLeft - 1
    }
    conn.prepareStatement(sql.toString())
  }

  /**
   * Saves a partition of a DataFrame to the JDBC database.  This is done in
   * a single database transaction in order to avoid repeatedly inserting
   * data as much as possible.
    *
    * 将DataFrame的分区保存到JDBC数据库,这是在单个数据库事务中完成的,以避免尽可能多地重复插入数据
   *
   * It is still theoretically possible for rows in a DataFrame to be
   * inserted into the database more than once if a stage somehow fails after
   * the commit occurs but before the stage can return successfully.
    *
    * 理论上,如果某个阶段在提交发生后但在阶段成功返回之前某个阶段失败,那么DataFrame中的行可能会多次插入到数据库中
   *
   * This is not a closure inside saveTable() because apparently cosmetic
   * implementation changes elsewhere might easily render such a closure
   * non-Serializable.  Instead, we explicitly close over all variables that
   * are used.
    *
    * 这不是saveTable()中的闭包,因为在其他地方显然化妆实现更改可能很容易使得这样的闭包不可序列化,
    * 相反,我们明确地关闭所有使用的变量。
   */
  def savePartition(
      getConnection: () => Connection,
      table: String,
      iterator: Iterator[Row],
      rddSchema: StructType,
      nullTypes: Array[Int]): Iterator[Byte] = {
    val conn = getConnection()
    var committed = false
    try {
      conn.setAutoCommit(false) // Everything in the same db transaction.
      val stmt = insertStatement(conn, table, rddSchema)
      try {
        while (iterator.hasNext) {
          val row = iterator.next()
          val numFields = rddSchema.fields.length
          var i = 0
          while (i < numFields) {
            if (row.isNullAt(i)) {
              stmt.setNull(i + 1, nullTypes(i))
            } else {
              rddSchema.fields(i).dataType match {
                case IntegerType => stmt.setInt(i + 1, row.getInt(i))
                case LongType => stmt.setLong(i + 1, row.getLong(i))
                case DoubleType => stmt.setDouble(i + 1, row.getDouble(i))
                case FloatType => stmt.setFloat(i + 1, row.getFloat(i))
                case ShortType => stmt.setInt(i + 1, row.getShort(i))
                case ByteType => stmt.setInt(i + 1, row.getByte(i))
                case BooleanType => stmt.setBoolean(i + 1, row.getBoolean(i))
                case StringType => stmt.setString(i + 1, row.getString(i))
                case BinaryType => stmt.setBytes(i + 1, row.getAs[Array[Byte]](i))
                case TimestampType => stmt.setTimestamp(i + 1, row.getAs[java.sql.Timestamp](i))
                case DateType => stmt.setDate(i + 1, row.getAs[java.sql.Date](i))
                case t: DecimalType => stmt.setBigDecimal(i + 1, row.getDecimal(i))
                case _ => throw new IllegalArgumentException(
                  s"Can't translate non-null value for field $i")
              }
            }
            i = i + 1
          }
          stmt.executeUpdate()
        }
      } finally {
        stmt.close()
      }
      conn.commit()
      committed = true
    } finally {
      if (!committed) {
        // The stage must fail.  We got here through an exception path, so
        // let the exception through unless rollback() or close() want to
        // tell the user about another problem.
        conn.rollback()
        conn.close()
      } else {
        // The stage must succeed.  We cannot propagate any exception close() might throw.
        try {
          conn.close()
        } catch {
          case e: Exception => logWarning("Transaction succeeded, but closing failed", e)
        }
      }
    }
    Array[Byte]().iterator
  }

  /**
   * Compute the schema string for this RDD.
    * 计算此RDD的模式字符串
   */
  def schemaString(df: DataFrame, url: String): String = {
    val sb = new StringBuilder()
    val dialect = JdbcDialects.get(url)
    df.schema.fields foreach { field => {
      val name = field.name
      val typ: String =
        dialect.getJDBCType(field.dataType).map(_.databaseTypeDefinition).getOrElse(
          field.dataType match {
            case IntegerType => "INTEGER"
            case LongType => "BIGINT"
            case DoubleType => "DOUBLE PRECISION"
            case FloatType => "REAL"
            case ShortType => "INTEGER"
            case ByteType => "BYTE"
            case BooleanType => "BIT(1)"
            case StringType => "TEXT"
            case BinaryType => "BLOB"
            case TimestampType => "TIMESTAMP"
            case DateType => "DATE"
            case t: DecimalType => s"DECIMAL(${t.precision},${t.scale})"
            case _ => throw new IllegalArgumentException(s"Don't know how to save $field to JDBC")
          })
      val nullable = if (field.nullable) "" else "NOT NULL"
      sb.append(s", $name $typ $nullable")
    }}
    if (sb.length < 2) "" else sb.substring(2)
  }

  /**
   * Saves the RDD to the database in a single transaction.
    * 在单个事务中将RDD保存到数据库
   */
  def saveTable(
      df: DataFrame,
      url: String,
      table: String,
      properties: Properties = new Properties()) {
    val dialect = JdbcDialects.get(url)
    val nullTypes: Array[Int] = df.schema.fields.map { field =>
      dialect.getJDBCType(field.dataType).map(_.jdbcNullType).getOrElse(
        field.dataType match {
          case IntegerType => java.sql.Types.INTEGER
          case LongType => java.sql.Types.BIGINT
          case DoubleType => java.sql.Types.DOUBLE
          case FloatType => java.sql.Types.REAL
          case ShortType => java.sql.Types.INTEGER
          case ByteType => java.sql.Types.INTEGER
          case BooleanType => java.sql.Types.BIT
          case StringType => java.sql.Types.CLOB
          case BinaryType => java.sql.Types.BLOB
          case TimestampType => java.sql.Types.TIMESTAMP
          case DateType => java.sql.Types.DATE
          case t: DecimalType => java.sql.Types.DECIMAL
          case _ => throw new IllegalArgumentException(
            s"Can't translate null value for field $field")
        })
    }

    val rddSchema = df.schema
    val driver: String = DriverRegistry.getDriverClassName(url)
    val getConnection: () => Connection = JDBCRDD.getConnector(driver, url, properties)
    df.foreachPartition { iterator =>
      savePartition(getConnection, table, iterator, rddSchema, nullTypes)
    }
  }

}
