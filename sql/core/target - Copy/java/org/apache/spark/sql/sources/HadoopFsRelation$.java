package org.apache.spark.sql.sources;
// no position
  class HadoopFsRelation$ implements org.apache.spark.Logging {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final HadoopFsRelation$ MODULE$ = null;
  public   HadoopFsRelation$ () { throw new RuntimeException(); }
  public  org.apache.hadoop.fs.FileStatus[] listLeafFiles (org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.fs.FileStatus status) { throw new RuntimeException(); }
  public  scala.collection.immutable.Set<org.apache.hadoop.fs.FileStatus> listLeafFilesInParallel (java.lang.String[] paths, org.apache.hadoop.conf.Configuration hadoopConf, org.apache.spark.SparkContext sparkContext) { throw new RuntimeException(); }
}
