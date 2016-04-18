package org.apache.spark.sql.sources;
/**
 * ::Experimental::
 * Implemented by objects that produce relations for a specific kind of data source
 * with a given schema and partitioned columns.  When Spark SQL is given a DDL operation with a
 * USING clause specified (to specify the implemented {@link HadoopFsRelationProvider}), a user defined
 * schema, and an optional list of partition columns, this interface is used to pass in the
 * parameters specified by a user.
 * <p>
 * Users may specify the fully qualified class name of a given data source.  When that class is
 * not found Spark SQL will append the class name <code>DefaultSource</code> to the path, allowing for
 * less verbose invocation.  For example, 'org.apache.spark.sql.json' would resolve to the
 * data source 'org.apache.spark.sql.json.DefaultSource'
 * <p>
 * A new instance of this class will be instantiated each time a DDL call is made.
 * <p>
 * The difference between a {@link RelationProvider} and a {@link HadoopFsRelationProvider} is
 * that users need to provide a schema and a (possibly empty) list of partition columns when
 * using a {@link HadoopFsRelationProvider}. A relation provider can inherits both {@link RelationProvider},
 * and {@link HadoopFsRelationProvider} if it can support schema inference, user-specified
 * schemas, and accessing partitioned relations.
 * <p>
 * @since 1.4.0
 */
public  interface HadoopFsRelationProvider {
  /**
   * Returns a new base relation with the given parameters, a user defined schema, and a list of
   * partition columns. Note: the parameters' keywords are case insensitive and this insensitivity
   * is enforced by the Map that is passed to the function.
   * <p>
   * @param dataSchema Schema of data columns (i.e., columns that are not partition columns).
   * @param sqlContext (undocumented)
   * @param paths (undocumented)
   * @param partitionColumns (undocumented)
   * @param parameters (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.sources.HadoopFsRelation createRelation (org.apache.spark.sql.SQLContext sqlContext, java.lang.String[] paths, scala.Option<org.apache.spark.sql.types.StructType> dataSchema, scala.Option<org.apache.spark.sql.types.StructType> partitionColumns, scala.collection.immutable.Map<java.lang.String, java.lang.String> parameters) ;
}
