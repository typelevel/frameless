package frameless.sql.rules

import frameless._
import frameless.functions.Lit
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{currentTimestamp, microsToInstant}
import org.apache.spark.sql.catalyst.expressions

import java.time.Instant

class DisabledFramelessLitRuleTests extends SQLRulesSuite {
  private val now: Long = currentTimestamp()

  test("java.sql.Timestamp no push-down") {
    val expectedStructure = X1(SQLTimestamp(now))

    noOptimizationRulesTest[SQLTimestamp](
      expectedStructure,
      { case e @ expressions.GreaterThanOrEqual(_, _: Lit[_]) => e },
      _ >= expectedStructure.a
    )
  }

  test("java.time.Instant no push-down") {
    val expectedStructure = X1(microsToInstant(now))

    noOptimizationRulesTest[Instant](
      expectedStructure,
      { case e@expressions.GreaterThanOrEqual(_, _: Lit[_]) => e },
      _ >= expectedStructure.a
    )
  }
}
