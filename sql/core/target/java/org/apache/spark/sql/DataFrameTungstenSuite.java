package org.apache.spark.sql;
/**
 * An end-to-end test suite specifically for testing Tungsten (Unsafe/CodeGen) mode.
 *&#x4e00;&#x79cd;&#x4e13;&#x95e8;&#x7528;&#x4e8e;&#x6d4b;&#x8bd5;&#x94a8;&#x6a21;&#x5f0f;&#x7684;&#x7ec8;&#x7aef;&#x5230;&#x7ec8;&#x7aef;&#x7684;&#x6d4b;&#x8bd5;&#x5957;&#x4ef6;
 Tungsten&#x9879;&#x76ee;&#x80fd;&#x591f;&#x5927;&#x5e45;&#x5ea6;&#x63d0;&#x9ad8;Spark&#x7684;&#x5185;&#x5b58;&#x548c;CPU&#x4f7f;&#x7528;&#x6548;&#x7387;&#xff0c;&#x4f7f;&#x5176;&#x6027;&#x80fd;&#x63a5;&#x8fd1;&#x4e8e;&#x786c;&#x4ef6;&#x7684;&#x6781;&#x9650;&#xff0c;&#x4e3b;&#x8981;&#x4f53;&#x73b0;&#x4ee5;&#x4e0b;&#x51e0;&#x70b9;&#xff1a;
 1.&#x5185;&#x5b58;&#x7ba1;&#x7406;&#x548c;&#x4e8c;&#x8fdb;&#x5236;&#x5904;&#x7406;&#xff0c;&#x5145;&#x5206;&#x5229;&#x7528;&#x5e94;&#x7528;&#x7a0b;&#x5e8f;&#x8bed;&#x4e49;&#x660e;&#x786e;&#x7ba1;&#x7406;&#x5185;&#x5b58;&#xff0c;&#x6d88;&#x9664;JVM&#x5bf9;&#x8c61;&#x6a21;&#x578b;&#x548c;&#x5783;&#x573e;&#x6536;&#x96c6;&#x673a;&#x5236;&#x7684;&#x5f00;&#x9500;&#x3002;
 2.&#x7f13;&#x5b58;&#x654f;&#x611f;&#x578b;&#x8ba1;&#x7b97;&#xff0c;&#x7b97;&#x6cd5;&#x548c;&#x6570;&#x636e;&#x7ed3;&#x6784;&#x90fd;&#x662f;&#x5229;&#x7528;&#x5185;&#x5b58;&#x5c42;&#x6b21;&#x7ed3;&#x6784;&#x3002;
 3.&#x4ee3;&#x7801;&#x751f;&#x6210;&#xff0c;&#x4f7f;&#x7528;&#x4ee3;&#x7801;&#x751f;&#x6210;&#x5668;&#x5145;&#x5206;&#x5229;&#x7528;&#x73b0;&#x4ee3;&#x7f16;&#x8bd1;&#x5668;&#x548c;CPU&#x3002;
 * This is here for now so I can make sure Tungsten project is tested without refactoring existing
 * end-to-end test infra. In the long run this should just go away.
 */
public  class DataFrameTungstenSuite extends org.apache.spark.sql.QueryTest implements org.apache.spark.sql.test.SharedSQLContext {
  public   DataFrameTungstenSuite () { throw new RuntimeException(); }
}
