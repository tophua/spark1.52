package org.apache.spark.sql.sources;
/**
 * ::DeveloperApi::
 * Implemented by objects that produce relations for a specific kind of data source.  When
 * Spark SQL is given a DDL operation with a USING clause specified (to specify the implemented
 * RelationProvider), this interface is used to pass in the parameters specified by a user.
 * <p>
 * Users may specify the fully qualified class name of a given data source.  When that class is
 * not found Spark SQL will append the class name <code>DefaultSource</code> to the path, allowing for
 * less verbose invocation.  For example, 'org.apache.spark.sql.json' would resolve to the
 * data source 'org.apache.spark.sql.json.DefaultSource'
 * <p>
 * A new instance of this class will be instantiated each time a DDL call is made.
 * <p>
 * @since 1.3.0
 */
public  interface RelationProvider {
  /**
   * Returns a new base relation with the given parameters.
   * Note: the parameters' keywords are case insensitive and this insensitivity is enforced
   * by the Map that is passed to the function.
   * @param sqlContext (undocumented)
   * @param parameters (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.sources.BaseRelation createRelation (org.apache.spark.sql.SQLContext sqlContext, scala.collection.immutable.Map<java.lang.String, java.lang.String> parameters) ;
}
