package org.apache.spark.sql.test;
/**
 * Helper trait that should be extended by all SQL test suites.
 * &#x8f85;&#x52a9;&#x6027;&#x7684;&#x7279;&#x70b9;,&#x5e94;&#x8be5;&#x5bf9;&#x6240;&#x6709;&#x7684;SQL&#x6d4b;&#x8bd5;&#x5957;&#x4ef6;&#x7684;&#x6269;&#x5c55;
 * This allows subclasses to plugin a custom {@link SQLContext}. It comes with test data
 * &#x8fd9;&#x4f7f;&#x5f97;&#x5b50;&#x7c7b;&#x53ef;&#x4ee5;&#x81ea;&#x5b9a;&#x4e49;{@link SQLContext}&#x63d2;&#x4ef6;,&#x5b83;&#x7684;&#x6d4b;&#x8bd5;&#x6570;&#x636e;&#x51c6;&#x5907;&#x4ee5;&#x53ca;&#x6240;&#x6709;&#x9690;&#x5f0f;&#x8f6c;&#x6362;&#x5e7f;&#x6cdb;&#x4f7f;&#x7528;&#x7684;dataframes
 * prepared in advance as well as all implicit conversions used extensively by dataframes.
 * To use implicit methods, import <code>testImplicits._</code> instead of through the {@link SQLContext}.
 * &#x4f7f;&#x7528;&#x9690;&#x5f0f;&#x65b9;&#x6cd5;,
 * <p>
 * Subclasses should *not* create {@link SQLContext}s in the test suite constructor, which is
 * prone to leaving multiple overlapping {@link org.apache.spark.SparkContext}s in the same JVM.
 */
  interface SQLTestUtils extends org.scalatest.BeforeAndAfterAll, org.apache.spark.sql.test.SQLTestData {
  public  org.apache.spark.sql.SQLContext _sqlContext () ;
  public  boolean loadTestDataBeforeTests () ;
  /**
   * =&gt;&#x7b26;&#x53f7;&#x53ef;&#x4ee5;&#x770b;&#x505a;&#x662f;&#x521b;&#x5efa;&#x51fd;&#x6570;&#x5b9e;&#x4f8b;&#x7684;&#x8bed;&#x6cd5;&#x7cd6;,
   * val sql: String =&gt; DataFrame &#x51fd;&#x6570;&#x7c7b;&#x578b;
   * &#x4f8b;&#x5982;&#xff1a; String =&gt; DataFrame &#x8868;&#x793a;&#x4e00;&#x4e2a;&#x51fd;&#x6570;&#x7684;&#x8f93;&#x5165;&#x53c2;&#x6570;&#x7c7b;&#x578b;&#x662f;String,&#x8fd4;&#x56de;&#x503c;&#x7c7b;&#x578b;&#x662f;DataFrame
   * @return (undocumented)
   */
  public  scala.Function1<java.lang.String, org.apache.spark.sql.DataFrame> sql () ;
  // no position
  protected  class testImplicits extends org.apache.spark.sql.SQLImplicits {
    /**
     * A helper object for importing SQL implicits.
     * &#x7528;&#x4e8e;&#x5bfc;&#x5165;SQL&#x9690;&#x5f0f;&#x7684;&#x5e2e;&#x52a9;&#x7a0b;&#x5e8f;&#x5bf9;&#x8c61;
     * <p>
     * Note that the alternative of importing <code>sqlContext.implicits._</code> is not possible here.
     * &#x6ce8;&#x610f;,&#x5bfc;&#x5165;<code>sqlContext.implicits._</code>&#x7684;&#x66ff;&#x4ee3;&#x65b9;&#x6cd5;&#x5728;&#x8fd9;&#x91cc;&#x662f;&#x4e0d;&#x53ef;&#x80fd;&#x7684;
     * This is because we create the {@link SQLContext} immediately before the first test is run,
     * &#x8fd9;&#x662f;&#x56e0;&#x4e3a;&#x6211;&#x4eec;&#x5728;&#x8fd0;&#x884c;&#x7b2c;&#x4e00;&#x4e2a;&#x6d4b;&#x8bd5;&#x4e4b;&#x524d;&#x7acb;&#x5373;&#x521b;&#x5efa;{@link SQLContext}
     * but the implicits import is needed in the constructor.
     * &#x4f46;&#x5728;&#x6784;&#x9020;&#x51fd;&#x6570;&#x4e2d;&#x9700;&#x8981;&#x8f93;&#x5165;implicits
     */
    public   testImplicits () { throw new RuntimeException(); }
    protected  org.apache.spark.sql.SQLContext _sqlContext () { throw new RuntimeException(); }
  }
  public  org.apache.spark.sql.test.SQLTestUtils.testImplicits$ testImplicits () ;
  public  void setupTestData () ;
  public  void beforeAll () ;
  /**
   * The Hadoop configuration used by the active {@link SQLContext}.
   * &#x4f7f;&#x7528;Hadoop&#x7684;&#x914d;&#x7f6e;&#x6fc0;&#x6d3b;[SQLContext]
   * @return (undocumented)
   */
  public  org.apache.hadoop.conf.Configuration configuration () ;
  /**
   * Sets all SQL configurations specified in <code>pairs</code>, calls <code>f</code>, and then restore all SQL
   * configurations.
   * &#x8bbe;&#x7f6e;&#x5728;&amp;ldquo;pair&amp;rdquo;&#x4e2d;&#x6307;&#x5b9a;&#x7684;&#x6240;&#x6709;SQL&#x914d;&#x7f6e;,&#x8c03;&#x7528;&amp;ldquo;f&amp;rdquo;,&#x7136;&#x540e;&#x8fd8;&#x539f;&#x6240;&#x6709;SQL&#x914d;&#x7f6e;
   * <p>
   * @todo Probably this method should be moved to a more general place
   * &#x53ef;&#x80fd;&#x8fd9;&#x4e2a;&#x65b9;&#x6cd5;&#x5e94;&#x8be5;&#x79fb;&#x52a8;&#x5230;&#x66f4;&#x4e00;&#x822c;&#x7684;&#x5730;&#x65b9;
   * @param pairs (undocumented)
   * @param f (undocumented)
   */
  public  void withSQLConf (scala.collection.Seq<scala.Tuple2<java.lang.String, java.lang.String>> pairs, scala.Function0<scala.runtime.BoxedUnit> f) ;
  /**
   * Generates a temporary path without creating the actual file/directory, then pass it to <code>f</code>. If
   * a file/directory is created there by <code>f</code>, it will be delete after <code>f</code> returns.
   * &#x751f;&#x6210;&#x4e00;&#x4e2a;&#x4e34;&#x65f6;&#x8def;&#x5f84;,&#x800c;&#x4e0d;&#x521b;&#x5efa;&#x5b9e;&#x9645;&#x7684;&#x6587;&#x4ef6;/&#x76ee;&#x5f55;,&#x7136;&#x540e;&#x628a;&#x5b83;&#x4f20;&#x7ed9;F,&#x5982;&#x679c;&#x4e00;&#x4e2a;&#x6587;&#x4ef6;/&#x76ee;&#x5f55;&#x662f;&#x7531;F&#x521b;&#x5efa;,&#x5b83;&#x5c06;&#x88ab;F&#x5220;&#x9664;&#x540e;&#x8fd4;&#x56de;
   * <p>
   * @todo Probably this method should be moved to a more general place
   * &#x4e5f;&#x8bb8;&#x8fd9;&#x4e2a;&#x65b9;&#x6cd5;&#x5e94;&#x8be5;&#x8f6c;&#x79fb;&#x5230;&#x4e00;&#x4e2a;&#x66f4;&#x666e;&#x904d;&#x7684;&#x5730;&#x65b9;
   * @param f (undocumented)
   */
  public  void withTempPath (scala.Function1<java.io.File, scala.runtime.BoxedUnit> f) ;
  /**
   * Creates a temporary directory, which is then passed to <code>f</code> and will be deleted after <code>f</code>
   * returns.
   * &#x521b;&#x5efa;&#x4e34;&#x65f6;&#x76ee;&#x5f55;,&#x7136;&#x540e;&#x4f20;&#x9012;&#x7ed9;f,&#x5c06;&#x88ab;&#x5220;&#x9664;&#x540e;f&#x8fd4;&#x56de;
   * @todo Probably this method should be moved to a more general place
   * &#x4e5f;&#x8bb8;&#x8fd9;&#x4e2a;&#x65b9;&#x6cd5;&#x5e94;&#x8be5;&#x8f6c;&#x79fb;&#x5230;&#x4e00;&#x4e2a;&#x66f4;&#x666e;&#x904d;&#x7684;&#x5730;&#x65b9;
   * @param f (undocumented)
   */
  public  void withTempDir (scala.Function1<java.io.File, scala.runtime.BoxedUnit> f) ;
  /**
   * Drops temporary table <code>tableName</code> after calling <code>f</code>.
   * &#x8c03;&#x7528;&#x540e;'f',&#x5220;&#x9664;&#x4e34;&#x65f6;&#x8868;'tableName'
   * &#x6ce8;&#x610f;:f&#x662f;&#x5b9a;&#x4e49;&#x7684;&#x65b9;&#x6cd5;
   * @param tableNames (undocumented)
   * @param f (undocumented)
   */
  public  void withTempTable (scala.collection.Seq<java.lang.String> tableNames, scala.Function0<scala.runtime.BoxedUnit> f) ;
  /**
   * Drops table <code>tableName</code> after calling <code>f</code>.
   * &#x8c03;&#x7528;f&#x540e;&#x5220;&#x9664;&#x8868;<code>tableName</code>,&#x5b9a;&#x4e49;&#x4e00;&#x4e2a;&#x67ef;&#x91cc;&#x5316;&#x9ad8;&#x9636;&#x51fd;&#x6570;
   * @param tableNames (undocumented)
   * @param f (undocumented)
   */
  public  void withTable (scala.collection.Seq<java.lang.String> tableNames, scala.Function0<scala.runtime.BoxedUnit> f) ;
  /**
   * Creates a temporary database and switches current database to it before executing <code>f</code>.  This
   * database is dropped after <code>f</code> returns.
   * &#x521b;&#x5efa;&#x4e00;&#x4e2a;&#x4e34;&#x65f6;&#x6570;&#x636e;&#x5e93;,&#x5e76;&#x5c06;&#x5f53;&#x524d;&#x6570;&#x636e;&#x5e93;&#x8f6c;&#x6362;&#x4e3a;&#x5b83;&#x4e4b;&#x524d;&#x6267;&#x884c;&#x7684;f,&#x8fd9;&#x4e2a;&#x6570;&#x636e;&#x5e93;&#x88ab;&#x5220;&#x9664;f&#x540e;&#x8fd4;&#x56de;&#x3002;
   * @param f (undocumented)
   */
  public  void withTempDatabase (scala.Function1<java.lang.String, scala.runtime.BoxedUnit> f) ;
  /**
   * Activates database <code>db</code> before executing <code>f</code>, then switches back to <code>default</code> database after
   * <code>f</code> returns.
   * &#x6fc0;&#x6d3b;&#x6570;&#x636e;&#x5e93;db&#x4e4b;&#x524d;&#x6267;&#x884c;f,&#x7136;&#x540e;&#x5207;&#x6362;&#x56de;&#x9ed8;&#x8ba4;&#x7684;&#x6570;&#x636e;&#x5e93;&#x540e;f&#x8fd4;&#x56de;
   * @param db (undocumented)
   * @param f (undocumented)
   */
  public  void activateDatabase (java.lang.String db, scala.Function0<scala.runtime.BoxedUnit> f) ;
  /**
   * Turn a logical plan into a {@link DataFrame}. This should be removed once we have an easier
   * way to construct {@link DataFrame} directly out of local data without relying on implicits.
   * &#x628a;&#x4e00;&#x4e2a;&#x903b;&#x8f91;&#x8ba1;&#x5212;&#x4e3a;DataFrame,&#x8fd9;&#x5e94;&#x8be5;&#x88ab;&#x5220;&#x9664;&#x4e00;&#x6b21;,&#x6211;&#x4eec;&#x6709;&#x4e00;&#x4e2a;&#x66f4;&#x5bb9;&#x6613;,
   * &#x5982;&#x4f55;&#x6784;&#x5efa;DataFrame&#x76f4;&#x63a5;&#x4ece;&#x672c;&#x5730;&#x6570;&#x636e;&#x4e0d;&#x4f9d;&#x8d56;&#x4e8e;&#x9690;&#x5f0f;&#x8f6c;&#x6362;
   * @param plan (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrame logicalPlanToSparkQuery (org.apache.spark.sql.catalyst.plans.logical.LogicalPlan plan) ;
}
