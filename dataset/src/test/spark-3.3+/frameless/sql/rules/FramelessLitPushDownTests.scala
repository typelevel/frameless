package frameless.sql.rules

import frameless._
import frameless.functions.Lit
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{currentTimestamp, microsToInstant}
import org.apache.spark.sql.sources.{EqualTo, GreaterThanOrEqual, IsNotNull}
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import java.time.Instant

class FramelessLitPushDownTests extends SQLRulesSuite {
  private val now: Long = currentTimestamp()

  test("java.sql.Timestamp push-down") {
    val expected = java.sql.Timestamp.from(microsToInstant(now))
    val expectedStructure = X1(SQLTimestamp(now))
    val expectedPushDownFilters = List(IsNotNull("a"), GreaterThanOrEqual("a", expected))

    predicatePushDownTest[SQLTimestamp](
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

    predicatePushDownTest[Instant](
      expectedStructure,
      expectedPushDownFilters,
      { case e @ expressions.GreaterThanOrEqual(_, _: Lit[_]) => e },
      _ >= expectedStructure.a
    )
  }

  test("struct push-down") {
    type Payload = X4[Int, Int, Int, Int]
    val expectedStructure = X1(X4(1, 2, 3, 4))
    val expected = new GenericRowWithSchema(Array(1, 2, 3, 4), TypedExpressionEncoder[Payload].schema)
    val expectedPushDownFilters = List(IsNotNull("a"), EqualTo("a", expected))

    predicatePushDownTest[Payload](
      expectedStructure,
      expectedPushDownFilters,
      { case e @ expressions.EqualTo(_, _: Lit[_]) => e },
      _ === expectedStructure.a
    )
  }
}
