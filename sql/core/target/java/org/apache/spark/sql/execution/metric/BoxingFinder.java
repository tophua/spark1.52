package org.apache.spark.sql.execution.metric;
/**
 * If <code>method</code> is null, search all methods of this class recursively to find if they do some boxing.
 * &#x5982;&#x679c;&amp;ldquo;&#x65b9;&#x6cd5;&amp;rdquo;&#x4e3a;&#x7a7a;,&#x5219;&#x9012;&#x5f52;&#x5730;&#x641c;&#x7d22;&#x8be5;&#x7c7b;&#x7684;&#x6240;&#x6709;&#x65b9;&#x6cd5;,&#x4ee5;&#x67e5;&#x627e;&#x5b83;&#x4eec;&#x662f;&#x5426;&#x6267;&#x884c;&#x4e86;&#x4e00;&#x4e9b;&#x88c5;&#x7bb1;
 * If <code>method</code> is specified, only search this method of the class to speed up the searching.
 * &#x5982;&#x679c;&#x6307;&#x5b9a;&#x4e86;&amp;ldquo;&#x65b9;&#x6cd5;&amp;rdquo;,&#x53ea;&#x80fd;&#x641c;&#x7d22;&#x8be5;&#x7c7b;&#x7684;&#x65b9;&#x6cd5;&#x4ee5;&#x52a0;&#x5feb;&#x641c;&#x7d22;&#x901f;&#x5ea6;
 * <p>
 * This method will skip the methods in <code>visitedMethods</code> to avoid potential infinite cycles.
 * &#x8be5;&#x65b9;&#x6cd5;&#x5c06;&#x8df3;&#x8fc7;&#x7684;&#x65b9;&#x6cd5;'visitedmethods'&#x907f;&#x514d;&#x6f5c;&#x5728;&#x7684;&#x65e0;&#x9650;&#x5faa;&#x73af;
 */
public  class BoxingFinder extends com.esotericsoftware.reflectasm.shaded.org.objectweb.asm.ClassVisitor {
  static public  scala.Option<com.esotericsoftware.reflectasm.shaded.org.objectweb.asm.ClassReader> getClassReader (java.lang.Class<?> cls) { throw new RuntimeException(); }
  public  scala.collection.mutable.Set<java.lang.String> boxingInvokes () { throw new RuntimeException(); }
  // not preceding
  public   BoxingFinder (org.apache.spark.sql.execution.metric.MethodIdentifier<?> method, scala.collection.mutable.Set<java.lang.String> boxingInvokes, scala.collection.mutable.Set<org.apache.spark.sql.execution.metric.MethodIdentifier<?>> visitedMethods) { throw new RuntimeException(); }
  private  scala.collection.immutable.Set<java.lang.String> primitiveBoxingClassName () { throw new RuntimeException(); }
  public  com.esotericsoftware.reflectasm.shaded.org.objectweb.asm.MethodVisitor visitMethod (int access, java.lang.String name, java.lang.String desc, java.lang.String sig, java.lang.String[] exceptions) { throw new RuntimeException(); }
}
