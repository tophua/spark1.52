package org.apache.spark.sql.execution.datasources.parquet;
/**
 * A convenient converter class for Parquet group types with an {@link HasParentContainerUpdater}.
 */
 abstract class CatalystGroupConverter extends org.apache.parquet.io.api.GroupConverter implements org.apache.spark.sql.execution.datasources.parquet.HasParentContainerUpdater {
  public  org.apache.spark.sql.execution.datasources.parquet.ParentContainerUpdater updater () { throw new RuntimeException(); }
  // not preceding
  public   CatalystGroupConverter (org.apache.spark.sql.execution.datasources.parquet.ParentContainerUpdater updater) { throw new RuntimeException(); }
}
