package frameless.sql.rules

import frameless._
import frameless.functions.Lit
import frameless.sql._
import frameless.sql.FramelessOptimizations
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{currentTimestamp, microsToInstant}
import org.apache.spark.sql.sources.{GreaterThanOrEqual, IsNotNull, EqualTo}
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType

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

  test("struct pushdown should not work") {
    type Payload = X4[Int, Int, Int, Int]
    val expectedStructure = X1(X4(1, 2, 3, 4))
    val expectedPushDownFilters = List(IsNotNull("a"))

    optimizationRulesTest[Payload](
      expectedStructure,
      expectedPushDownFilters,
      { case e@expressions.EqualTo(_, _: Lit[_]) => e },
      _ === expectedStructure.a
    )
  }
}

class FramelessLitExtensionsTests extends FramelessLitRuleTests {
  override def addSparkConfigProperties(config: SparkConf): Unit = {
    config.set("spark.sql.extensions", classOf[FramelessOptimizations].getName)
    ()
  }

  test("struct pushdown") {
    type Payload = X4[Int, Int, Int, Int]
    val expectedStructure = X1(X4(1, 2, 3, 4))
    val expected = new GenericRowWithSchema(Array(1, 2, 3, 4), implicitly[TypedEncoder[Payload]].catalystRepr.asInstanceOf[StructType])
    val expectedPushDownFilters = List(IsNotNull("a"), EqualTo("a", expected))

    optimizationRulesTest[Payload](
      expectedStructure,
      expectedPushDownFilters,
      { case e@expressions.EqualTo(_, _: Lit[_]) => e },
      _ === expectedStructure.a
    )
  }
}

