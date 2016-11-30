package org.apache.spark.sql.test;
/**
 * A collection of sample data used in SQL tests.
 * &#x4e00;&#x4e2a;&#x96c6;&#x5408;&#x6837;&#x672c;&#x6570;&#x636e;&#x7528;&#x4e8e;SQL&#x6d4b;&#x8bd5;
 */
  interface SQLTestData {
  // no position
  private  class internalImplicits$ extends org.apache.spark.sql.SQLImplicits {
    public   internalImplicits$ () { throw new RuntimeException(); }
    protected  org.apache.spark.sql.SQLContext _sqlContext () { throw new RuntimeException(); }
  }
  static public  class TestData implements scala.Product, scala.Serializable {
    public  int key () { throw new RuntimeException(); }
    public  java.lang.String value () { throw new RuntimeException(); }
    // not preceding
    public   TestData (int key, java.lang.String value) { throw new RuntimeException(); }
  }
  // no position
  static public  class TestData$ extends scala.runtime.AbstractFunction2<java.lang.Object, java.lang.String, org.apache.spark.sql.test.SQLTestData.TestData> implements scala.Serializable {
    /**
     * Static reference to the singleton instance of this Scala object.
     */
    public static final TestData$ MODULE$ = null;
    public   TestData$ () { throw new RuntimeException(); }
  }
  static public  class TestData2 implements scala.Product, scala.Serializable {
    public  int a () { throw new RuntimeException(); }
    public  int b () { throw new RuntimeException(); }
    // not preceding
    public   TestData2 (int a, int b) { throw new RuntimeException(); }
  }
  // no position
  static public  class TestData2$ extends scala.runtime.AbstractFunction2<java.lang.Object, java.lang.Object, org.apache.spark.sql.test.SQLTestData.TestData2> implements scala.Serializable {
    /**
     * Static reference to the singleton instance of this Scala object.
     */
    public static final TestData2$ MODULE$ = null;
    public   TestData2$ () { throw new RuntimeException(); }
  }
  static public  class TestData3 implements scala.Product, scala.Serializable {
    public  int a () { throw new RuntimeException(); }
    public  scala.Option<java.lang.Object> b () { throw new RuntimeException(); }
    // not preceding
    public   TestData3 (int a, scala.Option<java.lang.Object> b) { throw new RuntimeException(); }
  }
  // no position
  static public  class TestData3$ extends scala.runtime.AbstractFunction2<java.lang.Object, scala.Option<java.lang.Object>, org.apache.spark.sql.test.SQLTestData.TestData3> implements scala.Serializable {
    /**
     * Static reference to the singleton instance of this Scala object.
     */
    public static final TestData3$ MODULE$ = null;
    public   TestData3$ () { throw new RuntimeException(); }
  }
  static public  class LargeAndSmallInts implements scala.Product, scala.Serializable {
    public  int a () { throw new RuntimeException(); }
    public  int b () { throw new RuntimeException(); }
    // not preceding
    public   LargeAndSmallInts (int a, int b) { throw new RuntimeException(); }
  }
  // no position
  static public  class LargeAndSmallInts$ extends scala.runtime.AbstractFunction2<java.lang.Object, java.lang.Object, org.apache.spark.sql.test.SQLTestData.LargeAndSmallInts> implements scala.Serializable {
    /**
     * Static reference to the singleton instance of this Scala object.
     */
    public static final LargeAndSmallInts$ MODULE$ = null;
    public   LargeAndSmallInts$ () { throw new RuntimeException(); }
  }
  static public  class DecimalData implements scala.Product, scala.Serializable {
    public  scala.math.BigDecimal a () { throw new RuntimeException(); }
    public  scala.math.BigDecimal b () { throw new RuntimeException(); }
    // not preceding
    public   DecimalData (scala.math.BigDecimal a, scala.math.BigDecimal b) { throw new RuntimeException(); }
  }
  // no position
  static public  class DecimalData$ extends scala.runtime.AbstractFunction2<scala.math.BigDecimal, scala.math.BigDecimal, org.apache.spark.sql.test.SQLTestData.DecimalData> implements scala.Serializable {
    /**
     * Static reference to the singleton instance of this Scala object.
     */
    public static final DecimalData$ MODULE$ = null;
    public   DecimalData$ () { throw new RuntimeException(); }
  }
  static public  class BinaryData implements scala.Product, scala.Serializable {
    public  byte[] a () { throw new RuntimeException(); }
    public  int b () { throw new RuntimeException(); }
    // not preceding
    public   BinaryData (byte[] a, int b) { throw new RuntimeException(); }
  }
  // no position
  static public  class BinaryData$ extends scala.runtime.AbstractFunction2<byte[], java.lang.Object, org.apache.spark.sql.test.SQLTestData.BinaryData> implements scala.Serializable {
    /**
     * Static reference to the singleton instance of this Scala object.
     */
    public static final BinaryData$ MODULE$ = null;
    public   BinaryData$ () { throw new RuntimeException(); }
  }
  static public  class UpperCaseData implements scala.Product, scala.Serializable {
    public  int N () { throw new RuntimeException(); }
    public  java.lang.String L () { throw new RuntimeException(); }
    // not preceding
    public   UpperCaseData (int N, java.lang.String L) { throw new RuntimeException(); }
  }
  // no position
  static public  class UpperCaseData$ extends scala.runtime.AbstractFunction2<java.lang.Object, java.lang.String, org.apache.spark.sql.test.SQLTestData.UpperCaseData> implements scala.Serializable {
    /**
     * Static reference to the singleton instance of this Scala object.
     */
    public static final UpperCaseData$ MODULE$ = null;
    public   UpperCaseData$ () { throw new RuntimeException(); }
  }
  static public  class LowerCaseData implements scala.Product, scala.Serializable {
    public  int n () { throw new RuntimeException(); }
    public  java.lang.String l () { throw new RuntimeException(); }
    // not preceding
    public   LowerCaseData (int n, java.lang.String l) { throw new RuntimeException(); }
  }
  // no position
  static public  class LowerCaseData$ extends scala.runtime.AbstractFunction2<java.lang.Object, java.lang.String, org.apache.spark.sql.test.SQLTestData.LowerCaseData> implements scala.Serializable {
    /**
     * Static reference to the singleton instance of this Scala object.
     */
    public static final LowerCaseData$ MODULE$ = null;
    public   LowerCaseData$ () { throw new RuntimeException(); }
  }
  static public  class ArrayData implements scala.Product, scala.Serializable {
    public  scala.collection.Seq<java.lang.Object> data () { throw new RuntimeException(); }
    public  scala.collection.Seq<scala.collection.Seq<java.lang.Object>> nestedData () { throw new RuntimeException(); }
    // not preceding
    public   ArrayData (scala.collection.Seq<java.lang.Object> data, scala.collection.Seq<scala.collection.Seq<java.lang.Object>> nestedData) { throw new RuntimeException(); }
  }
  // no position
  static public  class ArrayData$ extends scala.runtime.AbstractFunction2<scala.collection.Seq<java.lang.Object>, scala.collection.Seq<scala.collection.Seq<java.lang.Object>>, org.apache.spark.sql.test.SQLTestData.ArrayData> implements scala.Serializable {
    /**
     * Static reference to the singleton instance of this Scala object.
     */
    public static final ArrayData$ MODULE$ = null;
    public   ArrayData$ () { throw new RuntimeException(); }
  }
  static public  class MapData implements scala.Product, scala.Serializable {
    public  scala.collection.Map<java.lang.Object, java.lang.String> data () { throw new RuntimeException(); }
    // not preceding
    public   MapData (scala.collection.Map<java.lang.Object, java.lang.String> data) { throw new RuntimeException(); }
  }
  // no position
  static public  class MapData$ extends scala.runtime.AbstractFunction1<scala.collection.Map<java.lang.Object, java.lang.String>, org.apache.spark.sql.test.SQLTestData.MapData> implements scala.Serializable {
    /**
     * Static reference to the singleton instance of this Scala object.
     */
    public static final MapData$ MODULE$ = null;
    public   MapData$ () { throw new RuntimeException(); }
  }
  static public  class StringData implements scala.Product, scala.Serializable {
    public  java.lang.String s () { throw new RuntimeException(); }
    // not preceding
    public   StringData (java.lang.String s) { throw new RuntimeException(); }
  }
  // no position
  static public  class StringData$ extends scala.runtime.AbstractFunction1<java.lang.String, org.apache.spark.sql.test.SQLTestData.StringData> implements scala.Serializable {
    /**
     * Static reference to the singleton instance of this Scala object.
     */
    public static final StringData$ MODULE$ = null;
    public   StringData$ () { throw new RuntimeException(); }
  }
  static public  class IntField implements scala.Product, scala.Serializable {
    public  int i () { throw new RuntimeException(); }
    // not preceding
    public   IntField (int i) { throw new RuntimeException(); }
  }
  // no position
  static public  class IntField$ extends scala.runtime.AbstractFunction1<java.lang.Object, org.apache.spark.sql.test.SQLTestData.IntField> implements scala.Serializable {
    /**
     * Static reference to the singleton instance of this Scala object.
     */
    public static final IntField$ MODULE$ = null;
    public   IntField$ () { throw new RuntimeException(); }
  }
  static public  class NullInts implements scala.Product, scala.Serializable {
    public  java.lang.Integer a () { throw new RuntimeException(); }
    // not preceding
    public   NullInts (java.lang.Integer a) { throw new RuntimeException(); }
  }
  // no position
  static public  class NullInts$ extends scala.runtime.AbstractFunction1<java.lang.Integer, org.apache.spark.sql.test.SQLTestData.NullInts> implements scala.Serializable {
    /**
     * Static reference to the singleton instance of this Scala object.
     */
    public static final NullInts$ MODULE$ = null;
    public   NullInts$ () { throw new RuntimeException(); }
  }
  static public  class NullStrings implements scala.Product, scala.Serializable {
    public  int n () { throw new RuntimeException(); }
    public  java.lang.String s () { throw new RuntimeException(); }
    // not preceding
    public   NullStrings (int n, java.lang.String s) { throw new RuntimeException(); }
  }
  // no position
  static public  class NullStrings$ extends scala.runtime.AbstractFunction2<java.lang.Object, java.lang.String, org.apache.spark.sql.test.SQLTestData.NullStrings> implements scala.Serializable {
    /**
     * Static reference to the singleton instance of this Scala object.
     */
    public static final NullStrings$ MODULE$ = null;
    public   NullStrings$ () { throw new RuntimeException(); }
  }
  static public  class TableName implements scala.Product, scala.Serializable {
    public  java.lang.String tableName () { throw new RuntimeException(); }
    // not preceding
    public   TableName (java.lang.String tableName) { throw new RuntimeException(); }
  }
  // no position
  static public  class TableName$ extends scala.runtime.AbstractFunction1<java.lang.String, org.apache.spark.sql.test.SQLTestData.TableName> implements scala.Serializable {
    /**
     * Static reference to the singleton instance of this Scala object.
     */
    public static final TableName$ MODULE$ = null;
    public   TableName$ () { throw new RuntimeException(); }
  }
  static public  class Person implements scala.Product, scala.Serializable {
    public  int id () { throw new RuntimeException(); }
    public  java.lang.String name () { throw new RuntimeException(); }
    public  int age () { throw new RuntimeException(); }
    // not preceding
    public   Person (int id, java.lang.String name, int age) { throw new RuntimeException(); }
  }
  // no position
  static public  class Person$ extends scala.runtime.AbstractFunction3<java.lang.Object, java.lang.String, java.lang.Object, org.apache.spark.sql.test.SQLTestData.Person> implements scala.Serializable {
    /**
     * Static reference to the singleton instance of this Scala object.
     */
    public static final Person$ MODULE$ = null;
    public   Person$ () { throw new RuntimeException(); }
  }
  static public  class Salary implements scala.Product, scala.Serializable {
    public  int personId () { throw new RuntimeException(); }
    public  double salary () { throw new RuntimeException(); }
    // not preceding
    public   Salary (int personId, double salary) { throw new RuntimeException(); }
  }
  // no position
  static public  class Salary$ extends scala.runtime.AbstractFunction2<java.lang.Object, java.lang.Object, org.apache.spark.sql.test.SQLTestData.Salary> implements scala.Serializable {
    /**
     * Static reference to the singleton instance of this Scala object.
     */
    public static final Salary$ MODULE$ = null;
    public   Salary$ () { throw new RuntimeException(); }
  }
  static public  class ComplexData implements scala.Product, scala.Serializable {
    public  scala.collection.immutable.Map<java.lang.String, java.lang.Object> m () { throw new RuntimeException(); }
    public  org.apache.spark.sql.test.SQLTestData.TestData s () { throw new RuntimeException(); }
    public  scala.collection.Seq<java.lang.Object> a () { throw new RuntimeException(); }
    public  boolean b () { throw new RuntimeException(); }
    // not preceding
    public   ComplexData (scala.collection.immutable.Map<java.lang.String, java.lang.Object> m, org.apache.spark.sql.test.SQLTestData.TestData s, scala.collection.Seq<java.lang.Object> a, boolean b) { throw new RuntimeException(); }
  }
  // no position
  static public  class ComplexData$ extends scala.runtime.AbstractFunction4<scala.collection.immutable.Map<java.lang.String, java.lang.Object>, org.apache.spark.sql.test.SQLTestData.TestData, scala.collection.Seq<java.lang.Object>, java.lang.Object, org.apache.spark.sql.test.SQLTestData.ComplexData> implements scala.Serializable {
    /**
     * Static reference to the singleton instance of this Scala object.
     */
    public static final ComplexData$ MODULE$ = null;
    public   ComplexData$ () { throw new RuntimeException(); }
  }
  public  org.apache.spark.sql.SQLContext _sqlContext () ;
  /**
   * Accessor for nested Scala object
   * @return (undocumented)
   */
  public  org.apache.spark.sql.test.SQLTestData.internalImplicits$ internalImplicits () ;
  public  org.apache.spark.sql.DataFrame testData () ;
  public  org.apache.spark.sql.DataFrame testData2 () ;
  public  org.apache.spark.sql.DataFrame testData3 () ;
  public  org.apache.spark.sql.DataFrame negativeData () ;
  public  org.apache.spark.sql.DataFrame largeAndSmallInts () ;
  public  org.apache.spark.sql.DataFrame decimalData () ;
  public  org.apache.spark.sql.DataFrame binaryData () ;
  public  org.apache.spark.sql.DataFrame upperCaseData () ;
  public  org.apache.spark.sql.DataFrame lowerCaseData () ;
  public  org.apache.spark.rdd.RDD<org.apache.spark.sql.test.SQLTestData.ArrayData> arrayData () ;
  public  org.apache.spark.rdd.RDD<org.apache.spark.sql.test.SQLTestData.MapData> mapData () ;
  public  org.apache.spark.rdd.RDD<org.apache.spark.sql.test.SQLTestData.StringData> repeatedData () ;
  public  org.apache.spark.rdd.RDD<org.apache.spark.sql.test.SQLTestData.StringData> nullableRepeatedData () ;
  public  org.apache.spark.sql.DataFrame nullInts () ;
  public  org.apache.spark.sql.DataFrame allNulls () ;
  public  org.apache.spark.sql.DataFrame nullStrings () ;
  public  org.apache.spark.sql.DataFrame tableName () ;
  public  org.apache.spark.rdd.RDD<java.lang.String> unparsedStrings () ;
  public  org.apache.spark.rdd.RDD<org.apache.spark.sql.test.SQLTestData.IntField> withEmptyParts () ;
  public  org.apache.spark.sql.DataFrame person () ;
  public  org.apache.spark.sql.DataFrame salary () ;
  public  org.apache.spark.sql.DataFrame complexData () ;
  /**
   * Initialize all test data such that all temp tables are properly registered.
   */
  public  void loadTestData () ;
}
