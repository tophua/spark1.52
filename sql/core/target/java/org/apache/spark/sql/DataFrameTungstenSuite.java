package org.apache.spark.sql;
/**
 * An end-to-end test suite specifically for testing Tungsten (Unsafe/CodeGen) mode.
 * <p>
 * This is here for now so I can make sure Tungsten project is tested without refactoring existing
 * end-to-end test infra. In the long run this should just go away.
 */
public  class DataFrameTungstenSuite extends org.apache.spark.sql.QueryTest implements org.apache.spark.sql.test.SharedSQLContext {
  public   DataFrameTungstenSuite () { throw new RuntimeException(); }
}
