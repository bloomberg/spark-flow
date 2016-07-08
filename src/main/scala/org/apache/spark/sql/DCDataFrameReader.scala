package org.apache.spark.sql

import java.util.Properties

import com.bloomberg.sparkflow.dc.{DataframeSourceDC, DC}
import org.apache.hadoop.util.StringUtils
import org.apache.spark.sql.types.StructType

/**
  * Created by ngoehausen on 4/26/16.
  */
class DCDataFrameReader(implicit rowEncoder: Encoder[Row]) {
  /**
    * Specifies the input data source format.
    *
    * @since 1.4.0
    */
  def format(source: String): DCDataFrameReader = {
    this.sourceOpt = Some(source)
    this
  }

  /**
    * Specifies the input schema. Some data sources (e.g. JSON) can infer the input schema
    * automatically from data. By specifying the schema here, the underlying data source can
    * skip the schema inference step, and thus speed up data loading.
    *
    * @since 1.4.0
    */
  def schema(schema: StructType): DCDataFrameReader = {
    this.userSpecifiedSchema = Option(schema)
    this
  }

  /**
    * Adds an input option for the underlying data source.
    *
    * @since 1.4.0
    */
  def option(key: String, value: String): DCDataFrameReader = {
    this.extraOptions += (key -> value)
    this
  }

  /**
    * (Scala-specific) Adds input options for the underlying data source.
    *
    * @since 1.4.0
    */
  def options(options: scala.collection.Map[String, String]): DCDataFrameReader = {
    this.extraOptions ++= options
    this
  }

  /**
    * Adds input options for the underlying data source.
    *
    * @since 1.4.0
    */
  def options(options: java.util.Map[String, String]): DCDataFrameReader = {
    this.options(options)
    this
  }

  /**
    * Loads input in as a [[DataFrame]], for data sources that require a path (e.g. data backed by
    * a local or distributed file system).
    *
    * @since 1.4.0
    */
  // TODO: Remove this one in Spark 2.0.
  def load(path: String): DC[Row] = {
    option("path", path).load()
  }

  /**
    * Loads input in as a [[DataFrame]], for data sources that don't require a path (e.g. external
    * key-value stores).
    *
    * @since 1.4.0
    */
  def load(): DC[Row] = {
    val f = (sparkSession: SparkSession) => {
      val withOptions = sparkSession.read.options(extraOptions)
      val withSource = sourceOpt match {
        case Some(source) => withOptions.format(source)
        case None => withOptions
      }
      val withSchema = userSpecifiedSchema match {
        case Some(schema) => withSource.schema(schema)
        case None => withOptions
      }
      withSchema.load()
    }
    new DataframeSourceDC(f, extraOptions("path"), extraOptions.toMap)
  }

  /**
    * Loads input in as a [[DataFrame]], for data sources that support multiple paths.
    * Only works if the source is a HadoopFsRelationProvider.
    *
    * @since 1.6.0
    */
  @scala.annotation.varargs
  def load(paths: String*): DC[Row] = {
    option("paths", paths.map(StringUtils.escapeString(_, '\\', ',')).mkString(",")).load()
  }

  /**
    * Construct a [[DataFrame]] representing the database table accessible via JDBC URL
    * url named table and connection properties.
    *
    * @since 1.4.0
    */
  def jdbc(url: String, table: String, properties: Properties): DC[Row] = {
    val f = (sparkSession: SparkSession) => {
      sparkSession.read.jdbc(url, table, properties)
    }
    new DataframeSourceDC(f, extraOptions("path"), extraOptions.toMap)
  }

  /**
    * Construct a [[DataFrame]] representing the database table accessible via JDBC URL
    * url named table. Partitions of the table will be retrieved in parallel based on the parameters
    * passed to this function.
    *
    * Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash
    * your external database systems.
    *
    * @param url JDBC database url of the form `jdbc:subprotocol:subname`
    * @param table Name of the table in the external database.
    * @param columnName the name of a column of integral type that will be used for partitioning.
    * @param lowerBound the minimum value of `columnName` used to decide partition stride
    * @param upperBound the maximum value of `columnName` used to decide partition stride
    * @param numPartitions the number of partitions.  the range `minValue`-`maxValue` will be split
    *                      evenly into this many partitions
    * @param connectionProperties JDBC database connection arguments, a list of arbitrary string
    *                             tag/value. Normally at least a "user" and "password" property
    *                             should be included.
    * @since 1.4.0
    */
  def jdbc(
            url: String,
            table: String,
            columnName: String,
            lowerBound: Long,
            upperBound: Long,
            numPartitions: Int,
            connectionProperties: Properties): DC[Row] = {
    val f = (sparkSession: SparkSession) => {
      sparkSession.read.jdbc(url, table, columnName, lowerBound, upperBound, numPartitions, connectionProperties)
    }
    new DataframeSourceDC(f, extraOptions("path"), extraOptions.toMap)
  }

  /**
    * Construct a [[DataFrame]] representing the database table accessible via JDBC URL
    * url named table using connection properties. The `predicates` parameter gives a list
    * expressions suitable for inclusion in WHERE clauses; each one defines one partition
    * of the [[DataFrame]].
    *
    * Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash
    * your external database systems.
    *
    * @param url JDBC database url of the form `jdbc:subprotocol:subname`
    * @param table Name of the table in the external database.
    * @param predicates Condition in the where clause for each partition.
    * @param connectionProperties JDBC database connection arguments, a list of arbitrary string
    *                             tag/value. Normally at least a "user" and "password" property
    *                             should be included.
    * @since 1.4.0
    */
  def jdbc(
            url: String,
            table: String,
            predicates: Array[String],
            connectionProperties: Properties): DC[Row] = {
    val f = (sparkSession: SparkSession) => {
      sparkSession.read.jdbc(url, table, predicates, connectionProperties)
    }
    new DataframeSourceDC(f, extraOptions("path"), extraOptions.toMap)
  }


  /**
    * Loads a JSON file (one object per line) and returns the result as a [[DataFrame]].
    *
    * This function goes through the input once to determine the input schema. If you know the
    * schema in advance, use the version that specifies the schema to avoid the extra scan.
    *
    * You can set the following JSON-specific options to deal with non-standard JSON files:
    * <li>`primitivesAsString` (default `false`): infers all primitive values as a string type</li>
    * <li>`allowComments` (default `false`): ignores Java/C++ style comment in JSON records</li>
    * <li>`allowUnquotedFieldNames` (default `false`): allows unquoted JSON field names</li>
    * <li>`allowSingleQuotes` (default `true`): allows single quotes in addition to double quotes
    * </li>
    * <li>`allowNumericLeadingZeros` (default `false`): allows leading zeros in numbers
    * (e.g. 00012)</li>
    *
    * @since 1.4.0
    */
  // TODO: Remove this one in Spark 2.0.
  def json(path: String): DC[Row] = format("json").load(path)

  /**
    * Loads a JSON file (one object per line) and returns the result as a [[DataFrame]].
    *
    * This function goes through the input once to determine the input schema. If you know the
    * schema in advance, use the version that specifies the schema to avoid the extra scan.
    *
    * You can set the following JSON-specific options to deal with non-standard JSON files:
    * <li>`primitivesAsString` (default `false`): infers all primitive values as a string type</li>
    * <li>`allowComments` (default `false`): ignores Java/C++ style comment in JSON records</li>
    * <li>`allowUnquotedFieldNames` (default `false`): allows unquoted JSON field names</li>
    * <li>`allowSingleQuotes` (default `true`): allows single quotes in addition to double quotes
    * </li>
    * <li>`allowNumericLeadingZeros` (default `false`): allows leading zeros in numbers
    * (e.g. 00012)</li>
    *
    * @since 1.6.0
    */
  def json(paths: String*): DC[Row] = format("json").load(paths : _*)

//  /**
//    * Loads an `JavaRDD[String]` storing JSON objects (one object per record) and
//    * returns the result as a [[DataFrame]].
//    *
//    * Unless the schema is specified using [[schema]] function, this function goes through the
//    * input once to determine the input schema.
//    *
//    * @param jsonRDD input RDD with one JSON object per record
//    * @since 1.4.0
//    */
//  def json(jsonRDD: JavaRDD[String]): DC[Row] = json(jsonRDD.rdd)
//
//  /**
//    * Loads an `RDD[String]` storing JSON objects (one object per record) and
//    * returns the result as a [[DataFrame]].
//    *
//    * Unless the schema is specified using [[schema]] function, this function goes through the
//    * input once to determine the input schema.
//    *
//    * @param jsonRDD input RDD with one JSON object per record
//    * @since 1.4.0
//    */
//  def json(jsonRDD: RDD[String]): DC[Row] = {
//    val f = (sqlContext: SQLContext) => {
//      sqlContext.baseRelationToDataFrame(
//        new JSONRelation(
//          Some(jsonRDD),
//          maybeDataSchema = userSpecifiedSchema,
//          maybePartitionSpec = None,
//          userDefinedPartitionColumns = None,
//          parameters = extraOptions.toMap)(sqlContext)
//      )
//    }
//    new DataframeSourceDC(f, extraOptions("path"))
//  }

  /**
    * Loads a Parquet file, returning the result as a [[DataFrame]]. This function returns an empty
    * [[DataFrame]] if no paths are passed in.
    *
    * @since 1.4.0
    */
  @scala.annotation.varargs
  def parquet(paths: String*): DC[Row] = {
    val f = (sparkSession: SparkSession) => {
      sparkSession.read.parquet(paths:_*)
    }
    new DataframeSourceDC(f, paths.mkString(";"), extraOptions.toMap)
  }

  /**
    * Loads an ORC file and returns the result as a [[DataFrame]].
    *
    * @param path input path
    * @since 1.5.0
    * @note Currently, this method can only be used together with `HiveContext`.
    */
  def orc(path: String): DC[Row] = format("orc").load(path)

  /**
    * Returns the specified table as a [[DataFrame]].
    *
    * @since 1.4.0
    */
  def table(tableName: String): DC[Row] = {
    val f = (sparkSession: SparkSession) => {
      sparkSession.table(tableName)
    }
    // TODO: this breaks signature lineage
    new DataframeSourceDC(f, tableName, extraOptions.toMap)
  }

  /**
    * Loads a text file and returns a [[DataFrame]] with a single string column named "value".
    * Each line in the text file is a new row in the resulting DataFrame. For example:
    * {{{
    *   // Scala:
    *   sqlContext.read.text("/path/to/spark/README.md")
    *
    *   // Java:
    *   sqlContext.read().text("/path/to/spark/README.md")
    * }}}
    *
    * @param paths input path
    * @since 1.6.0
    */
  @scala.annotation.varargs
  def text(paths: String*): DC[Row] = format("text").load(paths : _*)

  private var sourceOpt: Option[String] = None

  private var userSpecifiedSchema: Option[StructType] = None

  private var extraOptions = new scala.collection.mutable.HashMap[String, String]
}
