package frameless

/**
 * Type for the internal Spark representation of SQL date. If the `spark.sql.functions` where typed,
 * [date_add][1] would for instance be defined as `def date_add(d: SQLDate, i: Int); SQLDate`.
 *
 * [1]: https://spark.apache.org/docs/2.0.2/api/java/org/apache/spark/sql/functions.html#add_months(org.apache.spark.sql.Column,%20int)
 */
case class SQLDate(days: Int)
