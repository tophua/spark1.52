package org.apache.spark.sql.execution;
/**
 * An interface for relations that are backed by files.  When a class implements this interface,
 * the list of paths that it returns will be returned to a user who calls <code>inputPaths</code> on any
 * DataFrame that queries this relation.
 */
  interface FileRelation {
  /** Returns the list of files that will be read when scanning this relation. */
  public  java.lang.String[] inputFiles () ;
}
