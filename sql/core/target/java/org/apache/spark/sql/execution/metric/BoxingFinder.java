package org.apache.spark.sql.execution.metric;
/**
 * If <code>method</code> is null, search all methods of this class recursively to find if they do some boxing.
 * If <code>method</code> is specified, only search this method of the class to speed up the searching.
 * <p>
 * This method will skip the methods in <code>visitedMethods</code> to avoid potential infinite cycles.
 */
public  class BoxingFinder extends com.esotericsoftware.reflectasm.shaded.org.objectweb.asm.ClassVisitor {
  static public  scala.Option<com.esotericsoftware.reflectasm.shaded.org.objectweb.asm.ClassReader> getClassReader (java.lang.Class<?> cls) { throw new RuntimeException(); }
  public  scala.collection.mutable.Set<java.lang.String> boxingInvokes () { throw new RuntimeException(); }
  // not preceding
  public   BoxingFinder (org.apache.spark.sql.execution.metric.MethodIdentifier<?> method, scala.collection.mutable.Set<java.lang.String> boxingInvokes, scala.collection.mutable.Set<org.apache.spark.sql.execution.metric.MethodIdentifier<?>> visitedMethods) { throw new RuntimeException(); }
  private  scala.collection.immutable.Set<java.lang.String> primitiveBoxingClassName () { throw new RuntimeException(); }
  public  com.esotericsoftware.reflectasm.shaded.org.objectweb.asm.MethodVisitor visitMethod (int access, java.lang.String name, java.lang.String desc, java.lang.String sig, java.lang.String[] exceptions) { throw new RuntimeException(); }
}
