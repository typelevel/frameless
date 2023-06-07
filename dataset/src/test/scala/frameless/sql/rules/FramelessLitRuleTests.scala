package frameless.sql.rules

import frameless._
import frameless.functions.Lit
import frameless.sql._
import frameless.sql.FramelessOptimizations
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{currentTimestamp, microsToInstant}
import org.apache.spark.sql.sources.{GreaterThanOrEqual, IsNotNull}
import org.apache.spark.sql.catalyst.expressions

import java.time.Instant

trait FramelessLitRuleTests extends SQLRulesSuite {
  private val now: Long = currentTimestamp()

  test("java.sql.Timestamp push-down") {
    val expected = java.sql.Timestamp.from(microsToInstant(now))
    val expectedStructure = X1(SQLTimestamp(now))
    val expectedPushDownFilters = List(IsNotNull("a"), GreaterThanOrEqual("a", expected))

    optimizationRulesTest[SQLTimestamp](
      expectedStructure,
      expectedPushDownFilters,
      { case e @ expressions.GreaterThanOrEqual(_, _: Lit[_]) => e },
      _ >= expectedStructure.a
    )
  }

  test("java.time.Instant push-down") {
    val expected = java.sql.Timestamp.from(microsToInstant(now))
    val expectedStructure = X1(microsToInstant(now))
    val expectedPushDownFilters = List(IsNotNull("a"), GreaterThanOrEqual("a", expected))

    optimizationRulesTest[Instant](
      expectedStructure,
      expectedPushDownFilters,
      { case e @ expressions.GreaterThanOrEqual(_, _: Lit[_]) => e },
      _ >= expectedStructure.a
    )
  }
}

class FramelessLitOptimizationRuleTests extends FramelessLitRuleTests {
  override def registerOptimizations(sqlContext: SQLContext): Unit =
    LiteralRule.registerOptimizations(sqlContext)
}

class FramelessLitExtensionsTests extends FramelessLitRuleTests {
  override def addSparkConfigProperties(config: SparkConf): Unit = {
    config.set("spark.sql.extensions", classOf[FramelessOptimizations].getName)
    ()
  }
}

