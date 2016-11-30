package org.apache.spark.sql.execution.datasources;
/**
 * A parser for foreign DDL commands.
 */
public  class DDLParser extends org.apache.spark.sql.catalyst.AbstractSparkSQLParser implements org.apache.spark.sql.types.DataTypeParser, org.apache.spark.Logging {
  public   DDLParser (scala.Function1<java.lang.String, org.apache.spark.sql.catalyst.plans.logical.LogicalPlan> parseQuery) { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.plans.logical.LogicalPlan parse (java.lang.String input, boolean exceptionOnError) { throw new RuntimeException(); }
  protected  org.apache.spark.sql.catalyst.AbstractSparkSQLParser.Keyword CREATE () { throw new RuntimeException(); }
  protected  org.apache.spark.sql.catalyst.AbstractSparkSQLParser.Keyword TEMPORARY () { throw new RuntimeException(); }
  protected  org.apache.spark.sql.catalyst.AbstractSparkSQLParser.Keyword TABLE () { throw new RuntimeException(); }
  protected  org.apache.spark.sql.catalyst.AbstractSparkSQLParser.Keyword IF () { throw new RuntimeException(); }
  protected  org.apache.spark.sql.catalyst.AbstractSparkSQLParser.Keyword NOT () { throw new RuntimeException(); }
  protected  org.apache.spark.sql.catalyst.AbstractSparkSQLParser.Keyword EXISTS () { throw new RuntimeException(); }
  protected  org.apache.spark.sql.catalyst.AbstractSparkSQLParser.Keyword USING () { throw new RuntimeException(); }
  protected  org.apache.spark.sql.catalyst.AbstractSparkSQLParser.Keyword OPTIONS () { throw new RuntimeException(); }
  protected  org.apache.spark.sql.catalyst.AbstractSparkSQLParser.Keyword DESCRIBE () { throw new RuntimeException(); }
  protected  org.apache.spark.sql.catalyst.AbstractSparkSQLParser.Keyword EXTENDED () { throw new RuntimeException(); }
  protected  org.apache.spark.sql.catalyst.AbstractSparkSQLParser.Keyword AS () { throw new RuntimeException(); }
  protected  org.apache.spark.sql.catalyst.AbstractSparkSQLParser.Keyword COMMENT () { throw new RuntimeException(); }
  protected  org.apache.spark.sql.catalyst.AbstractSparkSQLParser.Keyword REFRESH () { throw new RuntimeException(); }
  protected  scala.util.parsing.combinator.Parsers.Parser<org.apache.spark.sql.catalyst.plans.logical.LogicalPlan> ddl () { throw new RuntimeException(); }
  protected  scala.util.parsing.combinator.Parsers.Parser<org.apache.spark.sql.catalyst.plans.logical.LogicalPlan> start () { throw new RuntimeException(); }
  /**
   * <code>CREATE [TEMPORARY] TABLE avroTable [IF NOT EXISTS]
   * USING org.apache.spark.sql.avro
   * OPTIONS (path "../hive/src/test/resources/data/files/episodes.avro")</code>
   * or
   * <code>CREATE [TEMPORARY] TABLE avroTable(intField int, stringField string...) [IF NOT EXISTS]
   * USING org.apache.spark.sql.avro
   * OPTIONS (path "../hive/src/test/resources/data/files/episodes.avro")</code>
   * or
   * <code>CREATE [TEMPORARY] TABLE avroTable [IF NOT EXISTS]
   * USING org.apache.spark.sql.avro
   * OPTIONS (path "../hive/src/test/resources/data/files/episodes.avro")</code>
   * AS SELECT ...
   * @return (undocumented)
   */
  protected  scala.util.parsing.combinator.Parsers.Parser<org.apache.spark.sql.catalyst.plans.logical.LogicalPlan> createTable () { throw new RuntimeException(); }
  protected  scala.util.parsing.combinator.Parsers.Parser<org.apache.spark.sql.catalyst.TableIdentifier> tableIdentifier () { throw new RuntimeException(); }
  protected  scala.util.parsing.combinator.Parsers.Parser<scala.collection.Seq<org.apache.spark.sql.types.StructField>> tableCols () { throw new RuntimeException(); }
  protected  scala.util.parsing.combinator.Parsers.Parser<org.apache.spark.sql.catalyst.plans.logical.LogicalPlan> describeTable () { throw new RuntimeException(); }
  protected  scala.util.parsing.combinator.Parsers.Parser<org.apache.spark.sql.catalyst.plans.logical.LogicalPlan> refreshTable () { throw new RuntimeException(); }
  protected  scala.util.parsing.combinator.Parsers.Parser<scala.collection.immutable.Map<java.lang.String, java.lang.String>> options () { throw new RuntimeException(); }
  protected  scala.util.parsing.combinator.Parsers.Parser<java.lang.String> className () { throw new RuntimeException(); }
  public  scala.util.parsing.combinator.Parsers.Parser<java.lang.String> regexToParser (scala.util.matching.Regex regex) { throw new RuntimeException(); }
  protected  scala.util.parsing.combinator.Parsers.Parser<java.lang.String> optionPart () { throw new RuntimeException(); }
  protected  scala.util.parsing.combinator.Parsers.Parser<java.lang.String> optionName () { throw new RuntimeException(); }
  protected  scala.util.parsing.combinator.Parsers.Parser<scala.Tuple2<java.lang.String, java.lang.String>> pair () { throw new RuntimeException(); }
  protected  scala.util.parsing.combinator.Parsers.Parser<org.apache.spark.sql.types.StructField> column () { throw new RuntimeException(); }
}
