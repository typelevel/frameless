package frameless

/**
 * Type for the Spark internal representation of a timestamp. If the `spark.sql.functions` where
 * typed, [current_timestamp][1] would for instance be defined as `def current_timestamp():
 * SQLTimestamp`.
 *
 * [1]:
 * https://spark.apache.org/docs/1.6.2/api/java/org/apache/spark/sql/functions.html#current_timestamp()
 */
case class SQLTimestamp(us: Long)
