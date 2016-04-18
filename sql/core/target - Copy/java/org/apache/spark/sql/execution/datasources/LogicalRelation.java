package org.apache.spark.sql.execution.datasources;
/**
 * Used to link a {@link BaseRelation} in to a logical query plan.
 * <p>
 * Note that sometimes we need to use <code>LogicalRelation</code> to replace an existing leaf node without
 * changing the output attributes' IDs.  The <code>expectedOutputAttributes</code> parameter is used for
 * this purpose.  See https://issues.apache.org/jira/browse/SPARK-10741 for more details.
 */
  class LogicalRelation extends org.apache.spark.sql.catalyst.plans.logical.LeafNode implements org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation, scala.Product, scala.Serializable {
  public  org.apache.spark.sql.sources.BaseRelation relation () { throw new RuntimeException(); }
  public  scala.Option<scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute>> expectedOutputAttributes () { throw new RuntimeException(); }
  // not preceding
  public   LogicalRelation (org.apache.spark.sql.sources.BaseRelation relation, scala.Option<scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute>> expectedOutputAttributes) { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.AttributeReference> output () { throw new RuntimeException(); }
  public  boolean equals (Object other) { throw new RuntimeException(); }
  public  int hashCode () { throw new RuntimeException(); }
  public  boolean sameResult (org.apache.spark.sql.catalyst.plans.logical.LogicalPlan otherPlan) { throw new RuntimeException(); }
  public  scala.collection.Seq<java.lang.Object> cleanArgs () { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.plans.logical.Statistics statistics () { throw new RuntimeException(); }
  /** Used to lookup original attribute capitalization */
  public  org.apache.spark.sql.catalyst.expressions.AttributeMap<org.apache.spark.sql.catalyst.expressions.AttributeReference> attributeMap () { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.datasources.LogicalRelation newInstance () { throw new RuntimeException(); }
  public  java.lang.String simpleString () { throw new RuntimeException(); }
}
