package org.apache.spark.sql;
/**
 * Functions for registering user-defined functions. Use {@link SQLContext.udf} to access this.
 * <p>
 * @since 1.3.0
 */
public  class UDFRegistration implements org.apache.spark.Logging {
     UDFRegistration (org.apache.spark.sql.SQLContext sqlContext) { throw new RuntimeException(); }
  private  org.apache.spark.sql.catalyst.analysis.FunctionRegistry functionRegistry () { throw new RuntimeException(); }
  protected  void registerPython (java.lang.String name, org.apache.spark.sql.UserDefinedPythonFunction udf) { throw new RuntimeException(); }
  /**
   * Register a user-defined aggregate function (UDAF).
   * <p>
   * @param name the name of the UDAF.
   * @param udaf the UDAF needs to be registered.
   * @return the registered UDAF.
   */
  public  org.apache.spark.sql.expressions.UserDefinedAggregateFunction register (java.lang.String name, org.apache.spark.sql.expressions.UserDefinedAggregateFunction udaf) { throw new RuntimeException(); }
  /**
   * Register a Scala closure of 0 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   * @param name (undocumented)
   * @param func (undocumented)
   * @param evidence$1 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction register (java.lang.String name, scala.Function0<RT> func, scala.reflect.api.TypeTags.TypeTag<RT> evidence$1) { throw new RuntimeException(); }
  /**
   * Register a Scala closure of 1 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   * @param name (undocumented)
   * @param func (undocumented)
   * @param evidence$2 (undocumented)
   * @param evidence$3 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object, A1 extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction register (java.lang.String name, scala.Function1<A1, RT> func, scala.reflect.api.TypeTags.TypeTag<RT> evidence$2, scala.reflect.api.TypeTags.TypeTag<A1> evidence$3) { throw new RuntimeException(); }
  /**
   * Register a Scala closure of 2 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   * @param name (undocumented)
   * @param func (undocumented)
   * @param evidence$4 (undocumented)
   * @param evidence$5 (undocumented)
   * @param evidence$6 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object, A1 extends java.lang.Object, A2 extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction register (java.lang.String name, scala.Function2<A1, A2, RT> func, scala.reflect.api.TypeTags.TypeTag<RT> evidence$4, scala.reflect.api.TypeTags.TypeTag<A1> evidence$5, scala.reflect.api.TypeTags.TypeTag<A2> evidence$6) { throw new RuntimeException(); }
  /**
   * Register a Scala closure of 3 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   * @param name (undocumented)
   * @param func (undocumented)
   * @param evidence$7 (undocumented)
   * @param evidence$8 (undocumented)
   * @param evidence$9 (undocumented)
   * @param evidence$10 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object, A1 extends java.lang.Object, A2 extends java.lang.Object, A3 extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction register (java.lang.String name, scala.Function3<A1, A2, A3, RT> func, scala.reflect.api.TypeTags.TypeTag<RT> evidence$7, scala.reflect.api.TypeTags.TypeTag<A1> evidence$8, scala.reflect.api.TypeTags.TypeTag<A2> evidence$9, scala.reflect.api.TypeTags.TypeTag<A3> evidence$10) { throw new RuntimeException(); }
  /**
   * Register a Scala closure of 4 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   * @param name (undocumented)
   * @param func (undocumented)
   * @param evidence$11 (undocumented)
   * @param evidence$12 (undocumented)
   * @param evidence$13 (undocumented)
   * @param evidence$14 (undocumented)
   * @param evidence$15 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object, A1 extends java.lang.Object, A2 extends java.lang.Object, A3 extends java.lang.Object, A4 extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction register (java.lang.String name, scala.Function4<A1, A2, A3, A4, RT> func, scala.reflect.api.TypeTags.TypeTag<RT> evidence$11, scala.reflect.api.TypeTags.TypeTag<A1> evidence$12, scala.reflect.api.TypeTags.TypeTag<A2> evidence$13, scala.reflect.api.TypeTags.TypeTag<A3> evidence$14, scala.reflect.api.TypeTags.TypeTag<A4> evidence$15) { throw new RuntimeException(); }
  /**
   * Register a Scala closure of 5 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   * @param name (undocumented)
   * @param func (undocumented)
   * @param evidence$16 (undocumented)
   * @param evidence$17 (undocumented)
   * @param evidence$18 (undocumented)
   * @param evidence$19 (undocumented)
   * @param evidence$20 (undocumented)
   * @param evidence$21 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object, A1 extends java.lang.Object, A2 extends java.lang.Object, A3 extends java.lang.Object, A4 extends java.lang.Object, A5 extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction register (java.lang.String name, scala.Function5<A1, A2, A3, A4, A5, RT> func, scala.reflect.api.TypeTags.TypeTag<RT> evidence$16, scala.reflect.api.TypeTags.TypeTag<A1> evidence$17, scala.reflect.api.TypeTags.TypeTag<A2> evidence$18, scala.reflect.api.TypeTags.TypeTag<A3> evidence$19, scala.reflect.api.TypeTags.TypeTag<A4> evidence$20, scala.reflect.api.TypeTags.TypeTag<A5> evidence$21) { throw new RuntimeException(); }
  /**
   * Register a Scala closure of 6 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   * @param name (undocumented)
   * @param func (undocumented)
   * @param evidence$22 (undocumented)
   * @param evidence$23 (undocumented)
   * @param evidence$24 (undocumented)
   * @param evidence$25 (undocumented)
   * @param evidence$26 (undocumented)
   * @param evidence$27 (undocumented)
   * @param evidence$28 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object, A1 extends java.lang.Object, A2 extends java.lang.Object, A3 extends java.lang.Object, A4 extends java.lang.Object, A5 extends java.lang.Object, A6 extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction register (java.lang.String name, scala.Function6<A1, A2, A3, A4, A5, A6, RT> func, scala.reflect.api.TypeTags.TypeTag<RT> evidence$22, scala.reflect.api.TypeTags.TypeTag<A1> evidence$23, scala.reflect.api.TypeTags.TypeTag<A2> evidence$24, scala.reflect.api.TypeTags.TypeTag<A3> evidence$25, scala.reflect.api.TypeTags.TypeTag<A4> evidence$26, scala.reflect.api.TypeTags.TypeTag<A5> evidence$27, scala.reflect.api.TypeTags.TypeTag<A6> evidence$28) { throw new RuntimeException(); }
  /**
   * Register a Scala closure of 7 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   * @param name (undocumented)
   * @param func (undocumented)
   * @param evidence$29 (undocumented)
   * @param evidence$30 (undocumented)
   * @param evidence$31 (undocumented)
   * @param evidence$32 (undocumented)
   * @param evidence$33 (undocumented)
   * @param evidence$34 (undocumented)
   * @param evidence$35 (undocumented)
   * @param evidence$36 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object, A1 extends java.lang.Object, A2 extends java.lang.Object, A3 extends java.lang.Object, A4 extends java.lang.Object, A5 extends java.lang.Object, A6 extends java.lang.Object, A7 extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction register (java.lang.String name, scala.Function7<A1, A2, A3, A4, A5, A6, A7, RT> func, scala.reflect.api.TypeTags.TypeTag<RT> evidence$29, scala.reflect.api.TypeTags.TypeTag<A1> evidence$30, scala.reflect.api.TypeTags.TypeTag<A2> evidence$31, scala.reflect.api.TypeTags.TypeTag<A3> evidence$32, scala.reflect.api.TypeTags.TypeTag<A4> evidence$33, scala.reflect.api.TypeTags.TypeTag<A5> evidence$34, scala.reflect.api.TypeTags.TypeTag<A6> evidence$35, scala.reflect.api.TypeTags.TypeTag<A7> evidence$36) { throw new RuntimeException(); }
  /**
   * Register a Scala closure of 8 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   * @param name (undocumented)
   * @param func (undocumented)
   * @param evidence$37 (undocumented)
   * @param evidence$38 (undocumented)
   * @param evidence$39 (undocumented)
   * @param evidence$40 (undocumented)
   * @param evidence$41 (undocumented)
   * @param evidence$42 (undocumented)
   * @param evidence$43 (undocumented)
   * @param evidence$44 (undocumented)
   * @param evidence$45 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object, A1 extends java.lang.Object, A2 extends java.lang.Object, A3 extends java.lang.Object, A4 extends java.lang.Object, A5 extends java.lang.Object, A6 extends java.lang.Object, A7 extends java.lang.Object, A8 extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction register (java.lang.String name, scala.Function8<A1, A2, A3, A4, A5, A6, A7, A8, RT> func, scala.reflect.api.TypeTags.TypeTag<RT> evidence$37, scala.reflect.api.TypeTags.TypeTag<A1> evidence$38, scala.reflect.api.TypeTags.TypeTag<A2> evidence$39, scala.reflect.api.TypeTags.TypeTag<A3> evidence$40, scala.reflect.api.TypeTags.TypeTag<A4> evidence$41, scala.reflect.api.TypeTags.TypeTag<A5> evidence$42, scala.reflect.api.TypeTags.TypeTag<A6> evidence$43, scala.reflect.api.TypeTags.TypeTag<A7> evidence$44, scala.reflect.api.TypeTags.TypeTag<A8> evidence$45) { throw new RuntimeException(); }
  /**
   * Register a Scala closure of 9 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   * @param name (undocumented)
   * @param func (undocumented)
   * @param evidence$46 (undocumented)
   * @param evidence$47 (undocumented)
   * @param evidence$48 (undocumented)
   * @param evidence$49 (undocumented)
   * @param evidence$50 (undocumented)
   * @param evidence$51 (undocumented)
   * @param evidence$52 (undocumented)
   * @param evidence$53 (undocumented)
   * @param evidence$54 (undocumented)
   * @param evidence$55 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object, A1 extends java.lang.Object, A2 extends java.lang.Object, A3 extends java.lang.Object, A4 extends java.lang.Object, A5 extends java.lang.Object, A6 extends java.lang.Object, A7 extends java.lang.Object, A8 extends java.lang.Object, A9 extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction register (java.lang.String name, scala.Function9<A1, A2, A3, A4, A5, A6, A7, A8, A9, RT> func, scala.reflect.api.TypeTags.TypeTag<RT> evidence$46, scala.reflect.api.TypeTags.TypeTag<A1> evidence$47, scala.reflect.api.TypeTags.TypeTag<A2> evidence$48, scala.reflect.api.TypeTags.TypeTag<A3> evidence$49, scala.reflect.api.TypeTags.TypeTag<A4> evidence$50, scala.reflect.api.TypeTags.TypeTag<A5> evidence$51, scala.reflect.api.TypeTags.TypeTag<A6> evidence$52, scala.reflect.api.TypeTags.TypeTag<A7> evidence$53, scala.reflect.api.TypeTags.TypeTag<A8> evidence$54, scala.reflect.api.TypeTags.TypeTag<A9> evidence$55) { throw new RuntimeException(); }
  /**
   * Register a Scala closure of 10 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   * @param name (undocumented)
   * @param func (undocumented)
   * @param evidence$56 (undocumented)
   * @param evidence$57 (undocumented)
   * @param evidence$58 (undocumented)
   * @param evidence$59 (undocumented)
   * @param evidence$60 (undocumented)
   * @param evidence$61 (undocumented)
   * @param evidence$62 (undocumented)
   * @param evidence$63 (undocumented)
   * @param evidence$64 (undocumented)
   * @param evidence$65 (undocumented)
   * @param evidence$66 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object, A1 extends java.lang.Object, A2 extends java.lang.Object, A3 extends java.lang.Object, A4 extends java.lang.Object, A5 extends java.lang.Object, A6 extends java.lang.Object, A7 extends java.lang.Object, A8 extends java.lang.Object, A9 extends java.lang.Object, A10 extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction register (java.lang.String name, scala.Function10<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT> func, scala.reflect.api.TypeTags.TypeTag<RT> evidence$56, scala.reflect.api.TypeTags.TypeTag<A1> evidence$57, scala.reflect.api.TypeTags.TypeTag<A2> evidence$58, scala.reflect.api.TypeTags.TypeTag<A3> evidence$59, scala.reflect.api.TypeTags.TypeTag<A4> evidence$60, scala.reflect.api.TypeTags.TypeTag<A5> evidence$61, scala.reflect.api.TypeTags.TypeTag<A6> evidence$62, scala.reflect.api.TypeTags.TypeTag<A7> evidence$63, scala.reflect.api.TypeTags.TypeTag<A8> evidence$64, scala.reflect.api.TypeTags.TypeTag<A9> evidence$65, scala.reflect.api.TypeTags.TypeTag<A10> evidence$66) { throw new RuntimeException(); }
  /**
   * Register a Scala closure of 11 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   * @param name (undocumented)
   * @param func (undocumented)
   * @param evidence$67 (undocumented)
   * @param evidence$68 (undocumented)
   * @param evidence$69 (undocumented)
   * @param evidence$70 (undocumented)
   * @param evidence$71 (undocumented)
   * @param evidence$72 (undocumented)
   * @param evidence$73 (undocumented)
   * @param evidence$74 (undocumented)
   * @param evidence$75 (undocumented)
   * @param evidence$76 (undocumented)
   * @param evidence$77 (undocumented)
   * @param evidence$78 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object, A1 extends java.lang.Object, A2 extends java.lang.Object, A3 extends java.lang.Object, A4 extends java.lang.Object, A5 extends java.lang.Object, A6 extends java.lang.Object, A7 extends java.lang.Object, A8 extends java.lang.Object, A9 extends java.lang.Object, A10 extends java.lang.Object, A11 extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction register (java.lang.String name, scala.Function11<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, RT> func, scala.reflect.api.TypeTags.TypeTag<RT> evidence$67, scala.reflect.api.TypeTags.TypeTag<A1> evidence$68, scala.reflect.api.TypeTags.TypeTag<A2> evidence$69, scala.reflect.api.TypeTags.TypeTag<A3> evidence$70, scala.reflect.api.TypeTags.TypeTag<A4> evidence$71, scala.reflect.api.TypeTags.TypeTag<A5> evidence$72, scala.reflect.api.TypeTags.TypeTag<A6> evidence$73, scala.reflect.api.TypeTags.TypeTag<A7> evidence$74, scala.reflect.api.TypeTags.TypeTag<A8> evidence$75, scala.reflect.api.TypeTags.TypeTag<A9> evidence$76, scala.reflect.api.TypeTags.TypeTag<A10> evidence$77, scala.reflect.api.TypeTags.TypeTag<A11> evidence$78) { throw new RuntimeException(); }
  /**
   * Register a Scala closure of 12 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   * @param name (undocumented)
   * @param func (undocumented)
   * @param evidence$79 (undocumented)
   * @param evidence$80 (undocumented)
   * @param evidence$81 (undocumented)
   * @param evidence$82 (undocumented)
   * @param evidence$83 (undocumented)
   * @param evidence$84 (undocumented)
   * @param evidence$85 (undocumented)
   * @param evidence$86 (undocumented)
   * @param evidence$87 (undocumented)
   * @param evidence$88 (undocumented)
   * @param evidence$89 (undocumented)
   * @param evidence$90 (undocumented)
   * @param evidence$91 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object, A1 extends java.lang.Object, A2 extends java.lang.Object, A3 extends java.lang.Object, A4 extends java.lang.Object, A5 extends java.lang.Object, A6 extends java.lang.Object, A7 extends java.lang.Object, A8 extends java.lang.Object, A9 extends java.lang.Object, A10 extends java.lang.Object, A11 extends java.lang.Object, A12 extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction register (java.lang.String name, scala.Function12<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, RT> func, scala.reflect.api.TypeTags.TypeTag<RT> evidence$79, scala.reflect.api.TypeTags.TypeTag<A1> evidence$80, scala.reflect.api.TypeTags.TypeTag<A2> evidence$81, scala.reflect.api.TypeTags.TypeTag<A3> evidence$82, scala.reflect.api.TypeTags.TypeTag<A4> evidence$83, scala.reflect.api.TypeTags.TypeTag<A5> evidence$84, scala.reflect.api.TypeTags.TypeTag<A6> evidence$85, scala.reflect.api.TypeTags.TypeTag<A7> evidence$86, scala.reflect.api.TypeTags.TypeTag<A8> evidence$87, scala.reflect.api.TypeTags.TypeTag<A9> evidence$88, scala.reflect.api.TypeTags.TypeTag<A10> evidence$89, scala.reflect.api.TypeTags.TypeTag<A11> evidence$90, scala.reflect.api.TypeTags.TypeTag<A12> evidence$91) { throw new RuntimeException(); }
  /**
   * Register a Scala closure of 13 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   * @param name (undocumented)
   * @param func (undocumented)
   * @param evidence$92 (undocumented)
   * @param evidence$93 (undocumented)
   * @param evidence$94 (undocumented)
   * @param evidence$95 (undocumented)
   * @param evidence$96 (undocumented)
   * @param evidence$97 (undocumented)
   * @param evidence$98 (undocumented)
   * @param evidence$99 (undocumented)
   * @param evidence$100 (undocumented)
   * @param evidence$101 (undocumented)
   * @param evidence$102 (undocumented)
   * @param evidence$103 (undocumented)
   * @param evidence$104 (undocumented)
   * @param evidence$105 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object, A1 extends java.lang.Object, A2 extends java.lang.Object, A3 extends java.lang.Object, A4 extends java.lang.Object, A5 extends java.lang.Object, A6 extends java.lang.Object, A7 extends java.lang.Object, A8 extends java.lang.Object, A9 extends java.lang.Object, A10 extends java.lang.Object, A11 extends java.lang.Object, A12 extends java.lang.Object, A13 extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction register (java.lang.String name, scala.Function13<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, RT> func, scala.reflect.api.TypeTags.TypeTag<RT> evidence$92, scala.reflect.api.TypeTags.TypeTag<A1> evidence$93, scala.reflect.api.TypeTags.TypeTag<A2> evidence$94, scala.reflect.api.TypeTags.TypeTag<A3> evidence$95, scala.reflect.api.TypeTags.TypeTag<A4> evidence$96, scala.reflect.api.TypeTags.TypeTag<A5> evidence$97, scala.reflect.api.TypeTags.TypeTag<A6> evidence$98, scala.reflect.api.TypeTags.TypeTag<A7> evidence$99, scala.reflect.api.TypeTags.TypeTag<A8> evidence$100, scala.reflect.api.TypeTags.TypeTag<A9> evidence$101, scala.reflect.api.TypeTags.TypeTag<A10> evidence$102, scala.reflect.api.TypeTags.TypeTag<A11> evidence$103, scala.reflect.api.TypeTags.TypeTag<A12> evidence$104, scala.reflect.api.TypeTags.TypeTag<A13> evidence$105) { throw new RuntimeException(); }
  /**
   * Register a Scala closure of 14 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   * @param name (undocumented)
   * @param func (undocumented)
   * @param evidence$106 (undocumented)
   * @param evidence$107 (undocumented)
   * @param evidence$108 (undocumented)
   * @param evidence$109 (undocumented)
   * @param evidence$110 (undocumented)
   * @param evidence$111 (undocumented)
   * @param evidence$112 (undocumented)
   * @param evidence$113 (undocumented)
   * @param evidence$114 (undocumented)
   * @param evidence$115 (undocumented)
   * @param evidence$116 (undocumented)
   * @param evidence$117 (undocumented)
   * @param evidence$118 (undocumented)
   * @param evidence$119 (undocumented)
   * @param evidence$120 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object, A1 extends java.lang.Object, A2 extends java.lang.Object, A3 extends java.lang.Object, A4 extends java.lang.Object, A5 extends java.lang.Object, A6 extends java.lang.Object, A7 extends java.lang.Object, A8 extends java.lang.Object, A9 extends java.lang.Object, A10 extends java.lang.Object, A11 extends java.lang.Object, A12 extends java.lang.Object, A13 extends java.lang.Object, A14 extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction register (java.lang.String name, scala.Function14<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, RT> func, scala.reflect.api.TypeTags.TypeTag<RT> evidence$106, scala.reflect.api.TypeTags.TypeTag<A1> evidence$107, scala.reflect.api.TypeTags.TypeTag<A2> evidence$108, scala.reflect.api.TypeTags.TypeTag<A3> evidence$109, scala.reflect.api.TypeTags.TypeTag<A4> evidence$110, scala.reflect.api.TypeTags.TypeTag<A5> evidence$111, scala.reflect.api.TypeTags.TypeTag<A6> evidence$112, scala.reflect.api.TypeTags.TypeTag<A7> evidence$113, scala.reflect.api.TypeTags.TypeTag<A8> evidence$114, scala.reflect.api.TypeTags.TypeTag<A9> evidence$115, scala.reflect.api.TypeTags.TypeTag<A10> evidence$116, scala.reflect.api.TypeTags.TypeTag<A11> evidence$117, scala.reflect.api.TypeTags.TypeTag<A12> evidence$118, scala.reflect.api.TypeTags.TypeTag<A13> evidence$119, scala.reflect.api.TypeTags.TypeTag<A14> evidence$120) { throw new RuntimeException(); }
  /**
   * Register a Scala closure of 15 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   * @param name (undocumented)
   * @param func (undocumented)
   * @param evidence$121 (undocumented)
   * @param evidence$122 (undocumented)
   * @param evidence$123 (undocumented)
   * @param evidence$124 (undocumented)
   * @param evidence$125 (undocumented)
   * @param evidence$126 (undocumented)
   * @param evidence$127 (undocumented)
   * @param evidence$128 (undocumented)
   * @param evidence$129 (undocumented)
   * @param evidence$130 (undocumented)
   * @param evidence$131 (undocumented)
   * @param evidence$132 (undocumented)
   * @param evidence$133 (undocumented)
   * @param evidence$134 (undocumented)
   * @param evidence$135 (undocumented)
   * @param evidence$136 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object, A1 extends java.lang.Object, A2 extends java.lang.Object, A3 extends java.lang.Object, A4 extends java.lang.Object, A5 extends java.lang.Object, A6 extends java.lang.Object, A7 extends java.lang.Object, A8 extends java.lang.Object, A9 extends java.lang.Object, A10 extends java.lang.Object, A11 extends java.lang.Object, A12 extends java.lang.Object, A13 extends java.lang.Object, A14 extends java.lang.Object, A15 extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction register (java.lang.String name, scala.Function15<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, RT> func, scala.reflect.api.TypeTags.TypeTag<RT> evidence$121, scala.reflect.api.TypeTags.TypeTag<A1> evidence$122, scala.reflect.api.TypeTags.TypeTag<A2> evidence$123, scala.reflect.api.TypeTags.TypeTag<A3> evidence$124, scala.reflect.api.TypeTags.TypeTag<A4> evidence$125, scala.reflect.api.TypeTags.TypeTag<A5> evidence$126, scala.reflect.api.TypeTags.TypeTag<A6> evidence$127, scala.reflect.api.TypeTags.TypeTag<A7> evidence$128, scala.reflect.api.TypeTags.TypeTag<A8> evidence$129, scala.reflect.api.TypeTags.TypeTag<A9> evidence$130, scala.reflect.api.TypeTags.TypeTag<A10> evidence$131, scala.reflect.api.TypeTags.TypeTag<A11> evidence$132, scala.reflect.api.TypeTags.TypeTag<A12> evidence$133, scala.reflect.api.TypeTags.TypeTag<A13> evidence$134, scala.reflect.api.TypeTags.TypeTag<A14> evidence$135, scala.reflect.api.TypeTags.TypeTag<A15> evidence$136) { throw new RuntimeException(); }
  /**
   * Register a Scala closure of 16 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   * @param name (undocumented)
   * @param func (undocumented)
   * @param evidence$137 (undocumented)
   * @param evidence$138 (undocumented)
   * @param evidence$139 (undocumented)
   * @param evidence$140 (undocumented)
   * @param evidence$141 (undocumented)
   * @param evidence$142 (undocumented)
   * @param evidence$143 (undocumented)
   * @param evidence$144 (undocumented)
   * @param evidence$145 (undocumented)
   * @param evidence$146 (undocumented)
   * @param evidence$147 (undocumented)
   * @param evidence$148 (undocumented)
   * @param evidence$149 (undocumented)
   * @param evidence$150 (undocumented)
   * @param evidence$151 (undocumented)
   * @param evidence$152 (undocumented)
   * @param evidence$153 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object, A1 extends java.lang.Object, A2 extends java.lang.Object, A3 extends java.lang.Object, A4 extends java.lang.Object, A5 extends java.lang.Object, A6 extends java.lang.Object, A7 extends java.lang.Object, A8 extends java.lang.Object, A9 extends java.lang.Object, A10 extends java.lang.Object, A11 extends java.lang.Object, A12 extends java.lang.Object, A13 extends java.lang.Object, A14 extends java.lang.Object, A15 extends java.lang.Object, A16 extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction register (java.lang.String name, scala.Function16<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, RT> func, scala.reflect.api.TypeTags.TypeTag<RT> evidence$137, scala.reflect.api.TypeTags.TypeTag<A1> evidence$138, scala.reflect.api.TypeTags.TypeTag<A2> evidence$139, scala.reflect.api.TypeTags.TypeTag<A3> evidence$140, scala.reflect.api.TypeTags.TypeTag<A4> evidence$141, scala.reflect.api.TypeTags.TypeTag<A5> evidence$142, scala.reflect.api.TypeTags.TypeTag<A6> evidence$143, scala.reflect.api.TypeTags.TypeTag<A7> evidence$144, scala.reflect.api.TypeTags.TypeTag<A8> evidence$145, scala.reflect.api.TypeTags.TypeTag<A9> evidence$146, scala.reflect.api.TypeTags.TypeTag<A10> evidence$147, scala.reflect.api.TypeTags.TypeTag<A11> evidence$148, scala.reflect.api.TypeTags.TypeTag<A12> evidence$149, scala.reflect.api.TypeTags.TypeTag<A13> evidence$150, scala.reflect.api.TypeTags.TypeTag<A14> evidence$151, scala.reflect.api.TypeTags.TypeTag<A15> evidence$152, scala.reflect.api.TypeTags.TypeTag<A16> evidence$153) { throw new RuntimeException(); }
  /**
   * Register a Scala closure of 17 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   * @param name (undocumented)
   * @param func (undocumented)
   * @param evidence$154 (undocumented)
   * @param evidence$155 (undocumented)
   * @param evidence$156 (undocumented)
   * @param evidence$157 (undocumented)
   * @param evidence$158 (undocumented)
   * @param evidence$159 (undocumented)
   * @param evidence$160 (undocumented)
   * @param evidence$161 (undocumented)
   * @param evidence$162 (undocumented)
   * @param evidence$163 (undocumented)
   * @param evidence$164 (undocumented)
   * @param evidence$165 (undocumented)
   * @param evidence$166 (undocumented)
   * @param evidence$167 (undocumented)
   * @param evidence$168 (undocumented)
   * @param evidence$169 (undocumented)
   * @param evidence$170 (undocumented)
   * @param evidence$171 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object, A1 extends java.lang.Object, A2 extends java.lang.Object, A3 extends java.lang.Object, A4 extends java.lang.Object, A5 extends java.lang.Object, A6 extends java.lang.Object, A7 extends java.lang.Object, A8 extends java.lang.Object, A9 extends java.lang.Object, A10 extends java.lang.Object, A11 extends java.lang.Object, A12 extends java.lang.Object, A13 extends java.lang.Object, A14 extends java.lang.Object, A15 extends java.lang.Object, A16 extends java.lang.Object, A17 extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction register (java.lang.String name, scala.Function17<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, RT> func, scala.reflect.api.TypeTags.TypeTag<RT> evidence$154, scala.reflect.api.TypeTags.TypeTag<A1> evidence$155, scala.reflect.api.TypeTags.TypeTag<A2> evidence$156, scala.reflect.api.TypeTags.TypeTag<A3> evidence$157, scala.reflect.api.TypeTags.TypeTag<A4> evidence$158, scala.reflect.api.TypeTags.TypeTag<A5> evidence$159, scala.reflect.api.TypeTags.TypeTag<A6> evidence$160, scala.reflect.api.TypeTags.TypeTag<A7> evidence$161, scala.reflect.api.TypeTags.TypeTag<A8> evidence$162, scala.reflect.api.TypeTags.TypeTag<A9> evidence$163, scala.reflect.api.TypeTags.TypeTag<A10> evidence$164, scala.reflect.api.TypeTags.TypeTag<A11> evidence$165, scala.reflect.api.TypeTags.TypeTag<A12> evidence$166, scala.reflect.api.TypeTags.TypeTag<A13> evidence$167, scala.reflect.api.TypeTags.TypeTag<A14> evidence$168, scala.reflect.api.TypeTags.TypeTag<A15> evidence$169, scala.reflect.api.TypeTags.TypeTag<A16> evidence$170, scala.reflect.api.TypeTags.TypeTag<A17> evidence$171) { throw new RuntimeException(); }
  /**
   * Register a Scala closure of 18 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   * @param name (undocumented)
   * @param func (undocumented)
   * @param evidence$172 (undocumented)
   * @param evidence$173 (undocumented)
   * @param evidence$174 (undocumented)
   * @param evidence$175 (undocumented)
   * @param evidence$176 (undocumented)
   * @param evidence$177 (undocumented)
   * @param evidence$178 (undocumented)
   * @param evidence$179 (undocumented)
   * @param evidence$180 (undocumented)
   * @param evidence$181 (undocumented)
   * @param evidence$182 (undocumented)
   * @param evidence$183 (undocumented)
   * @param evidence$184 (undocumented)
   * @param evidence$185 (undocumented)
   * @param evidence$186 (undocumented)
   * @param evidence$187 (undocumented)
   * @param evidence$188 (undocumented)
   * @param evidence$189 (undocumented)
   * @param evidence$190 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object, A1 extends java.lang.Object, A2 extends java.lang.Object, A3 extends java.lang.Object, A4 extends java.lang.Object, A5 extends java.lang.Object, A6 extends java.lang.Object, A7 extends java.lang.Object, A8 extends java.lang.Object, A9 extends java.lang.Object, A10 extends java.lang.Object, A11 extends java.lang.Object, A12 extends java.lang.Object, A13 extends java.lang.Object, A14 extends java.lang.Object, A15 extends java.lang.Object, A16 extends java.lang.Object, A17 extends java.lang.Object, A18 extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction register (java.lang.String name, scala.Function18<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, RT> func, scala.reflect.api.TypeTags.TypeTag<RT> evidence$172, scala.reflect.api.TypeTags.TypeTag<A1> evidence$173, scala.reflect.api.TypeTags.TypeTag<A2> evidence$174, scala.reflect.api.TypeTags.TypeTag<A3> evidence$175, scala.reflect.api.TypeTags.TypeTag<A4> evidence$176, scala.reflect.api.TypeTags.TypeTag<A5> evidence$177, scala.reflect.api.TypeTags.TypeTag<A6> evidence$178, scala.reflect.api.TypeTags.TypeTag<A7> evidence$179, scala.reflect.api.TypeTags.TypeTag<A8> evidence$180, scala.reflect.api.TypeTags.TypeTag<A9> evidence$181, scala.reflect.api.TypeTags.TypeTag<A10> evidence$182, scala.reflect.api.TypeTags.TypeTag<A11> evidence$183, scala.reflect.api.TypeTags.TypeTag<A12> evidence$184, scala.reflect.api.TypeTags.TypeTag<A13> evidence$185, scala.reflect.api.TypeTags.TypeTag<A14> evidence$186, scala.reflect.api.TypeTags.TypeTag<A15> evidence$187, scala.reflect.api.TypeTags.TypeTag<A16> evidence$188, scala.reflect.api.TypeTags.TypeTag<A17> evidence$189, scala.reflect.api.TypeTags.TypeTag<A18> evidence$190) { throw new RuntimeException(); }
  /**
   * Register a Scala closure of 19 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   * @param name (undocumented)
   * @param func (undocumented)
   * @param evidence$191 (undocumented)
   * @param evidence$192 (undocumented)
   * @param evidence$193 (undocumented)
   * @param evidence$194 (undocumented)
   * @param evidence$195 (undocumented)
   * @param evidence$196 (undocumented)
   * @param evidence$197 (undocumented)
   * @param evidence$198 (undocumented)
   * @param evidence$199 (undocumented)
   * @param evidence$200 (undocumented)
   * @param evidence$201 (undocumented)
   * @param evidence$202 (undocumented)
   * @param evidence$203 (undocumented)
   * @param evidence$204 (undocumented)
   * @param evidence$205 (undocumented)
   * @param evidence$206 (undocumented)
   * @param evidence$207 (undocumented)
   * @param evidence$208 (undocumented)
   * @param evidence$209 (undocumented)
   * @param evidence$210 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object, A1 extends java.lang.Object, A2 extends java.lang.Object, A3 extends java.lang.Object, A4 extends java.lang.Object, A5 extends java.lang.Object, A6 extends java.lang.Object, A7 extends java.lang.Object, A8 extends java.lang.Object, A9 extends java.lang.Object, A10 extends java.lang.Object, A11 extends java.lang.Object, A12 extends java.lang.Object, A13 extends java.lang.Object, A14 extends java.lang.Object, A15 extends java.lang.Object, A16 extends java.lang.Object, A17 extends java.lang.Object, A18 extends java.lang.Object, A19 extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction register (java.lang.String name, scala.Function19<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, RT> func, scala.reflect.api.TypeTags.TypeTag<RT> evidence$191, scala.reflect.api.TypeTags.TypeTag<A1> evidence$192, scala.reflect.api.TypeTags.TypeTag<A2> evidence$193, scala.reflect.api.TypeTags.TypeTag<A3> evidence$194, scala.reflect.api.TypeTags.TypeTag<A4> evidence$195, scala.reflect.api.TypeTags.TypeTag<A5> evidence$196, scala.reflect.api.TypeTags.TypeTag<A6> evidence$197, scala.reflect.api.TypeTags.TypeTag<A7> evidence$198, scala.reflect.api.TypeTags.TypeTag<A8> evidence$199, scala.reflect.api.TypeTags.TypeTag<A9> evidence$200, scala.reflect.api.TypeTags.TypeTag<A10> evidence$201, scala.reflect.api.TypeTags.TypeTag<A11> evidence$202, scala.reflect.api.TypeTags.TypeTag<A12> evidence$203, scala.reflect.api.TypeTags.TypeTag<A13> evidence$204, scala.reflect.api.TypeTags.TypeTag<A14> evidence$205, scala.reflect.api.TypeTags.TypeTag<A15> evidence$206, scala.reflect.api.TypeTags.TypeTag<A16> evidence$207, scala.reflect.api.TypeTags.TypeTag<A17> evidence$208, scala.reflect.api.TypeTags.TypeTag<A18> evidence$209, scala.reflect.api.TypeTags.TypeTag<A19> evidence$210) { throw new RuntimeException(); }
  /**
   * Register a Scala closure of 20 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   * @param name (undocumented)
   * @param func (undocumented)
   * @param evidence$211 (undocumented)
   * @param evidence$212 (undocumented)
   * @param evidence$213 (undocumented)
   * @param evidence$214 (undocumented)
   * @param evidence$215 (undocumented)
   * @param evidence$216 (undocumented)
   * @param evidence$217 (undocumented)
   * @param evidence$218 (undocumented)
   * @param evidence$219 (undocumented)
   * @param evidence$220 (undocumented)
   * @param evidence$221 (undocumented)
   * @param evidence$222 (undocumented)
   * @param evidence$223 (undocumented)
   * @param evidence$224 (undocumented)
   * @param evidence$225 (undocumented)
   * @param evidence$226 (undocumented)
   * @param evidence$227 (undocumented)
   * @param evidence$228 (undocumented)
   * @param evidence$229 (undocumented)
   * @param evidence$230 (undocumented)
   * @param evidence$231 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object, A1 extends java.lang.Object, A2 extends java.lang.Object, A3 extends java.lang.Object, A4 extends java.lang.Object, A5 extends java.lang.Object, A6 extends java.lang.Object, A7 extends java.lang.Object, A8 extends java.lang.Object, A9 extends java.lang.Object, A10 extends java.lang.Object, A11 extends java.lang.Object, A12 extends java.lang.Object, A13 extends java.lang.Object, A14 extends java.lang.Object, A15 extends java.lang.Object, A16 extends java.lang.Object, A17 extends java.lang.Object, A18 extends java.lang.Object, A19 extends java.lang.Object, A20 extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction register (java.lang.String name, scala.Function20<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, RT> func, scala.reflect.api.TypeTags.TypeTag<RT> evidence$211, scala.reflect.api.TypeTags.TypeTag<A1> evidence$212, scala.reflect.api.TypeTags.TypeTag<A2> evidence$213, scala.reflect.api.TypeTags.TypeTag<A3> evidence$214, scala.reflect.api.TypeTags.TypeTag<A4> evidence$215, scala.reflect.api.TypeTags.TypeTag<A5> evidence$216, scala.reflect.api.TypeTags.TypeTag<A6> evidence$217, scala.reflect.api.TypeTags.TypeTag<A7> evidence$218, scala.reflect.api.TypeTags.TypeTag<A8> evidence$219, scala.reflect.api.TypeTags.TypeTag<A9> evidence$220, scala.reflect.api.TypeTags.TypeTag<A10> evidence$221, scala.reflect.api.TypeTags.TypeTag<A11> evidence$222, scala.reflect.api.TypeTags.TypeTag<A12> evidence$223, scala.reflect.api.TypeTags.TypeTag<A13> evidence$224, scala.reflect.api.TypeTags.TypeTag<A14> evidence$225, scala.reflect.api.TypeTags.TypeTag<A15> evidence$226, scala.reflect.api.TypeTags.TypeTag<A16> evidence$227, scala.reflect.api.TypeTags.TypeTag<A17> evidence$228, scala.reflect.api.TypeTags.TypeTag<A18> evidence$229, scala.reflect.api.TypeTags.TypeTag<A19> evidence$230, scala.reflect.api.TypeTags.TypeTag<A20> evidence$231) { throw new RuntimeException(); }
  /**
   * Register a Scala closure of 21 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   * @param name (undocumented)
   * @param func (undocumented)
   * @param evidence$232 (undocumented)
   * @param evidence$233 (undocumented)
   * @param evidence$234 (undocumented)
   * @param evidence$235 (undocumented)
   * @param evidence$236 (undocumented)
   * @param evidence$237 (undocumented)
   * @param evidence$238 (undocumented)
   * @param evidence$239 (undocumented)
   * @param evidence$240 (undocumented)
   * @param evidence$241 (undocumented)
   * @param evidence$242 (undocumented)
   * @param evidence$243 (undocumented)
   * @param evidence$244 (undocumented)
   * @param evidence$245 (undocumented)
   * @param evidence$246 (undocumented)
   * @param evidence$247 (undocumented)
   * @param evidence$248 (undocumented)
   * @param evidence$249 (undocumented)
   * @param evidence$250 (undocumented)
   * @param evidence$251 (undocumented)
   * @param evidence$252 (undocumented)
   * @param evidence$253 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object, A1 extends java.lang.Object, A2 extends java.lang.Object, A3 extends java.lang.Object, A4 extends java.lang.Object, A5 extends java.lang.Object, A6 extends java.lang.Object, A7 extends java.lang.Object, A8 extends java.lang.Object, A9 extends java.lang.Object, A10 extends java.lang.Object, A11 extends java.lang.Object, A12 extends java.lang.Object, A13 extends java.lang.Object, A14 extends java.lang.Object, A15 extends java.lang.Object, A16 extends java.lang.Object, A17 extends java.lang.Object, A18 extends java.lang.Object, A19 extends java.lang.Object, A20 extends java.lang.Object, A21 extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction register (java.lang.String name, scala.Function21<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21, RT> func, scala.reflect.api.TypeTags.TypeTag<RT> evidence$232, scala.reflect.api.TypeTags.TypeTag<A1> evidence$233, scala.reflect.api.TypeTags.TypeTag<A2> evidence$234, scala.reflect.api.TypeTags.TypeTag<A3> evidence$235, scala.reflect.api.TypeTags.TypeTag<A4> evidence$236, scala.reflect.api.TypeTags.TypeTag<A5> evidence$237, scala.reflect.api.TypeTags.TypeTag<A6> evidence$238, scala.reflect.api.TypeTags.TypeTag<A7> evidence$239, scala.reflect.api.TypeTags.TypeTag<A8> evidence$240, scala.reflect.api.TypeTags.TypeTag<A9> evidence$241, scala.reflect.api.TypeTags.TypeTag<A10> evidence$242, scala.reflect.api.TypeTags.TypeTag<A11> evidence$243, scala.reflect.api.TypeTags.TypeTag<A12> evidence$244, scala.reflect.api.TypeTags.TypeTag<A13> evidence$245, scala.reflect.api.TypeTags.TypeTag<A14> evidence$246, scala.reflect.api.TypeTags.TypeTag<A15> evidence$247, scala.reflect.api.TypeTags.TypeTag<A16> evidence$248, scala.reflect.api.TypeTags.TypeTag<A17> evidence$249, scala.reflect.api.TypeTags.TypeTag<A18> evidence$250, scala.reflect.api.TypeTags.TypeTag<A19> evidence$251, scala.reflect.api.TypeTags.TypeTag<A20> evidence$252, scala.reflect.api.TypeTags.TypeTag<A21> evidence$253) { throw new RuntimeException(); }
  /**
   * Register a Scala closure of 22 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   * @param name (undocumented)
   * @param func (undocumented)
   * @param evidence$254 (undocumented)
   * @param evidence$255 (undocumented)
   * @param evidence$256 (undocumented)
   * @param evidence$257 (undocumented)
   * @param evidence$258 (undocumented)
   * @param evidence$259 (undocumented)
   * @param evidence$260 (undocumented)
   * @param evidence$261 (undocumented)
   * @param evidence$262 (undocumented)
   * @param evidence$263 (undocumented)
   * @param evidence$264 (undocumented)
   * @param evidence$265 (undocumented)
   * @param evidence$266 (undocumented)
   * @param evidence$267 (undocumented)
   * @param evidence$268 (undocumented)
   * @param evidence$269 (undocumented)
   * @param evidence$270 (undocumented)
   * @param evidence$271 (undocumented)
   * @param evidence$272 (undocumented)
   * @param evidence$273 (undocumented)
   * @param evidence$274 (undocumented)
   * @param evidence$275 (undocumented)
   * @param evidence$276 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object, A1 extends java.lang.Object, A2 extends java.lang.Object, A3 extends java.lang.Object, A4 extends java.lang.Object, A5 extends java.lang.Object, A6 extends java.lang.Object, A7 extends java.lang.Object, A8 extends java.lang.Object, A9 extends java.lang.Object, A10 extends java.lang.Object, A11 extends java.lang.Object, A12 extends java.lang.Object, A13 extends java.lang.Object, A14 extends java.lang.Object, A15 extends java.lang.Object, A16 extends java.lang.Object, A17 extends java.lang.Object, A18 extends java.lang.Object, A19 extends java.lang.Object, A20 extends java.lang.Object, A21 extends java.lang.Object, A22 extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction register (java.lang.String name, scala.Function22<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21, A22, RT> func, scala.reflect.api.TypeTags.TypeTag<RT> evidence$254, scala.reflect.api.TypeTags.TypeTag<A1> evidence$255, scala.reflect.api.TypeTags.TypeTag<A2> evidence$256, scala.reflect.api.TypeTags.TypeTag<A3> evidence$257, scala.reflect.api.TypeTags.TypeTag<A4> evidence$258, scala.reflect.api.TypeTags.TypeTag<A5> evidence$259, scala.reflect.api.TypeTags.TypeTag<A6> evidence$260, scala.reflect.api.TypeTags.TypeTag<A7> evidence$261, scala.reflect.api.TypeTags.TypeTag<A8> evidence$262, scala.reflect.api.TypeTags.TypeTag<A9> evidence$263, scala.reflect.api.TypeTags.TypeTag<A10> evidence$264, scala.reflect.api.TypeTags.TypeTag<A11> evidence$265, scala.reflect.api.TypeTags.TypeTag<A12> evidence$266, scala.reflect.api.TypeTags.TypeTag<A13> evidence$267, scala.reflect.api.TypeTags.TypeTag<A14> evidence$268, scala.reflect.api.TypeTags.TypeTag<A15> evidence$269, scala.reflect.api.TypeTags.TypeTag<A16> evidence$270, scala.reflect.api.TypeTags.TypeTag<A17> evidence$271, scala.reflect.api.TypeTags.TypeTag<A18> evidence$272, scala.reflect.api.TypeTags.TypeTag<A19> evidence$273, scala.reflect.api.TypeTags.TypeTag<A20> evidence$274, scala.reflect.api.TypeTags.TypeTag<A21> evidence$275, scala.reflect.api.TypeTags.TypeTag<A22> evidence$276) { throw new RuntimeException(); }
  /**
   * Register a user-defined function with 1 arguments.
   * @since 1.3.0
   * @param name (undocumented)
   * @param f (undocumented)
   * @param returnType (undocumented)
   */
  public  void register (java.lang.String name, org.apache.spark.sql.api.java.UDF1<?, ?> f, org.apache.spark.sql.types.DataType returnType) { throw new RuntimeException(); }
  /**
   * Register a user-defined function with 2 arguments.
   * @since 1.3.0
   * @param name (undocumented)
   * @param f (undocumented)
   * @param returnType (undocumented)
   */
  public  void register (java.lang.String name, org.apache.spark.sql.api.java.UDF2<?, ?, ?> f, org.apache.spark.sql.types.DataType returnType) { throw new RuntimeException(); }
  /**
   * Register a user-defined function with 3 arguments.
   * @since 1.3.0
   * @param name (undocumented)
   * @param f (undocumented)
   * @param returnType (undocumented)
   */
  public  void register (java.lang.String name, org.apache.spark.sql.api.java.UDF3<?, ?, ?, ?> f, org.apache.spark.sql.types.DataType returnType) { throw new RuntimeException(); }
  /**
   * Register a user-defined function with 4 arguments.
   * @since 1.3.0
   * @param name (undocumented)
   * @param f (undocumented)
   * @param returnType (undocumented)
   */
  public  void register (java.lang.String name, org.apache.spark.sql.api.java.UDF4<?, ?, ?, ?, ?> f, org.apache.spark.sql.types.DataType returnType) { throw new RuntimeException(); }
  /**
   * Register a user-defined function with 5 arguments.
   * @since 1.3.0
   * @param name (undocumented)
   * @param f (undocumented)
   * @param returnType (undocumented)
   */
  public  void register (java.lang.String name, org.apache.spark.sql.api.java.UDF5<?, ?, ?, ?, ?, ?> f, org.apache.spark.sql.types.DataType returnType) { throw new RuntimeException(); }
  /**
   * Register a user-defined function with 6 arguments.
   * @since 1.3.0
   * @param name (undocumented)
   * @param f (undocumented)
   * @param returnType (undocumented)
   */
  public  void register (java.lang.String name, org.apache.spark.sql.api.java.UDF6<?, ?, ?, ?, ?, ?, ?> f, org.apache.spark.sql.types.DataType returnType) { throw new RuntimeException(); }
  /**
   * Register a user-defined function with 7 arguments.
   * @since 1.3.0
   * @param name (undocumented)
   * @param f (undocumented)
   * @param returnType (undocumented)
   */
  public  void register (java.lang.String name, org.apache.spark.sql.api.java.UDF7<?, ?, ?, ?, ?, ?, ?, ?> f, org.apache.spark.sql.types.DataType returnType) { throw new RuntimeException(); }
  /**
   * Register a user-defined function with 8 arguments.
   * @since 1.3.0
   * @param name (undocumented)
   * @param f (undocumented)
   * @param returnType (undocumented)
   */
  public  void register (java.lang.String name, org.apache.spark.sql.api.java.UDF8<?, ?, ?, ?, ?, ?, ?, ?, ?> f, org.apache.spark.sql.types.DataType returnType) { throw new RuntimeException(); }
  /**
   * Register a user-defined function with 9 arguments.
   * @since 1.3.0
   * @param name (undocumented)
   * @param f (undocumented)
   * @param returnType (undocumented)
   */
  public  void register (java.lang.String name, org.apache.spark.sql.api.java.UDF9<?, ?, ?, ?, ?, ?, ?, ?, ?, ?> f, org.apache.spark.sql.types.DataType returnType) { throw new RuntimeException(); }
  /**
   * Register a user-defined function with 10 arguments.
   * @since 1.3.0
   * @param name (undocumented)
   * @param f (undocumented)
   * @param returnType (undocumented)
   */
  public  void register (java.lang.String name, org.apache.spark.sql.api.java.UDF10<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> f, org.apache.spark.sql.types.DataType returnType) { throw new RuntimeException(); }
  /**
   * Register a user-defined function with 11 arguments.
   * @since 1.3.0
   * @param name (undocumented)
   * @param f (undocumented)
   * @param returnType (undocumented)
   */
  public  void register (java.lang.String name, org.apache.spark.sql.api.java.UDF11<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> f, org.apache.spark.sql.types.DataType returnType) { throw new RuntimeException(); }
  /**
   * Register a user-defined function with 12 arguments.
   * @since 1.3.0
   * @param name (undocumented)
   * @param f (undocumented)
   * @param returnType (undocumented)
   */
  public  void register (java.lang.String name, org.apache.spark.sql.api.java.UDF12<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> f, org.apache.spark.sql.types.DataType returnType) { throw new RuntimeException(); }
  /**
   * Register a user-defined function with 13 arguments.
   * @since 1.3.0
   * @param name (undocumented)
   * @param f (undocumented)
   * @param returnType (undocumented)
   */
  public  void register (java.lang.String name, org.apache.spark.sql.api.java.UDF13<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> f, org.apache.spark.sql.types.DataType returnType) { throw new RuntimeException(); }
  /**
   * Register a user-defined function with 14 arguments.
   * @since 1.3.0
   * @param name (undocumented)
   * @param f (undocumented)
   * @param returnType (undocumented)
   */
  public  void register (java.lang.String name, org.apache.spark.sql.api.java.UDF14<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> f, org.apache.spark.sql.types.DataType returnType) { throw new RuntimeException(); }
  /**
   * Register a user-defined function with 15 arguments.
   * @since 1.3.0
   * @param name (undocumented)
   * @param f (undocumented)
   * @param returnType (undocumented)
   */
  public  void register (java.lang.String name, org.apache.spark.sql.api.java.UDF15<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> f, org.apache.spark.sql.types.DataType returnType) { throw new RuntimeException(); }
  /**
   * Register a user-defined function with 16 arguments.
   * @since 1.3.0
   * @param name (undocumented)
   * @param f (undocumented)
   * @param returnType (undocumented)
   */
  public  void register (java.lang.String name, org.apache.spark.sql.api.java.UDF16<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> f, org.apache.spark.sql.types.DataType returnType) { throw new RuntimeException(); }
  /**
   * Register a user-defined function with 17 arguments.
   * @since 1.3.0
   * @param name (undocumented)
   * @param f (undocumented)
   * @param returnType (undocumented)
   */
  public  void register (java.lang.String name, org.apache.spark.sql.api.java.UDF17<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> f, org.apache.spark.sql.types.DataType returnType) { throw new RuntimeException(); }
  /**
   * Register a user-defined function with 18 arguments.
   * @since 1.3.0
   * @param name (undocumented)
   * @param f (undocumented)
   * @param returnType (undocumented)
   */
  public  void register (java.lang.String name, org.apache.spark.sql.api.java.UDF18<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> f, org.apache.spark.sql.types.DataType returnType) { throw new RuntimeException(); }
  /**
   * Register a user-defined function with 19 arguments.
   * @since 1.3.0
   * @param name (undocumented)
   * @param f (undocumented)
   * @param returnType (undocumented)
   */
  public  void register (java.lang.String name, org.apache.spark.sql.api.java.UDF19<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> f, org.apache.spark.sql.types.DataType returnType) { throw new RuntimeException(); }
  /**
   * Register a user-defined function with 20 arguments.
   * @since 1.3.0
   * @param name (undocumented)
   * @param f (undocumented)
   * @param returnType (undocumented)
   */
  public  void register (java.lang.String name, org.apache.spark.sql.api.java.UDF20<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> f, org.apache.spark.sql.types.DataType returnType) { throw new RuntimeException(); }
  /**
   * Register a user-defined function with 21 arguments.
   * @since 1.3.0
   * @param name (undocumented)
   * @param f (undocumented)
   * @param returnType (undocumented)
   */
  public  void register (java.lang.String name, org.apache.spark.sql.api.java.UDF21<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> f, org.apache.spark.sql.types.DataType returnType) { throw new RuntimeException(); }
  /**
   * Register a user-defined function with 22 arguments.
   * @since 1.3.0
   * @param name (undocumented)
   * @param f (undocumented)
   * @param returnType (undocumented)
   */
  public  void register (java.lang.String name, org.apache.spark.sql.api.java.UDF22<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> f, org.apache.spark.sql.types.DataType returnType) { throw new RuntimeException(); }
}
