package org.apache.spark.sql.execution.datasources;
/**
 * Used to represent the operation of create table using a data source.
 * param:  allowExisting If it is true, we will do nothing when the table already exists.
 *                      If it is false, an exception will be thrown
 */
public  class CreateTableUsing extends org.apache.spark.sql.catalyst.plans.logical.LogicalPlan implements org.apache.spark.sql.catalyst.plans.logical.Command, scala.Product, scala.Serializable {
  public  org.apache.spark.sql.catalyst.TableIdentifier tableIdent () { throw new RuntimeException(); }
  public  scala.Option<org.apache.spark.sql.types.StructType> userSpecifiedSchema () { throw new RuntimeException(); }
  public  java.lang.String provider () { throw new RuntimeException(); }
  public  boolean temporary () { throw new RuntimeException(); }
  public  scala.collection.immutable.Map<java.lang.String, java.lang.String> options () { throw new RuntimeException(); }
  public  boolean allowExisting () { throw new RuntimeException(); }
  public  boolean managedIfNoPath () { throw new RuntimeException(); }
  // not preceding
  public   CreateTableUsing (org.apache.spark.sql.catalyst.TableIdentifier tableIdent, scala.Option<org.apache.spark.sql.types.StructType> userSpecifiedSchema, java.lang.String provider, boolean temporary, scala.collection.immutable.Map<java.lang.String, java.lang.String> options, boolean allowExisting, boolean managedIfNoPath) { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.plans.logical.LogicalPlan> children () { throw new RuntimeException(); }
}
