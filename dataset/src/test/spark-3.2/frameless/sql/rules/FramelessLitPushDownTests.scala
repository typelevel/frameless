package frameless.sql.rules

import frameless._
import frameless.sql._
import frameless.functions.Lit
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{
  currentTimestamp,
  microsToInstant
}
import org.apache.spark.sql.sources.{ Filter, IsNotNull }
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{
  Cast,
  Expression,
  GenericRowWithSchema
}
import java.time.Instant

import org.apache.spark.sql.catalyst.plans.logical
import org.scalatest.Assertion

//Note as InvokeLike and "ConditionalExpression" don't have SPARK-40380 and SPARK-39106 no predicate pushdowns can happen in 3.2.4
class FramelessLitPushDownTests extends SQLRulesSuite {
  private val now: Long = currentTimestamp()

  test("java.sql.Timestamp push-down") {
    val expected = java.sql.Timestamp.from(microsToInstant(now))
    val expectedStructure = X1(SQLTimestamp(now))
    val expectedPushDownFilters = List(IsNotNull("a"))

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
    val expectedPushDownFilters = List(IsNotNull("a"))

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
    val expected = new GenericRowWithSchema(
      Array(1, 2, 3, 4),
      TypedExpressionEncoder[Payload].schema
    )
    val expectedPushDownFilters = List(IsNotNull("a"))

    predicatePushDownTest[Payload](
      expectedStructure,
      expectedPushDownFilters,
      // Cast not Lit because of SPARK-40380
      { case e @ expressions.EqualTo(_, _: Cast) => e },
      _ === expectedStructure.a
    )
  }

  override def predicatePushDownTest[A: TypedEncoder: CatalystOrdered](
      expected: X1[A],
      expectedPushDownFilters: List[Filter],
      planShouldContain: PartialFunction[Expression, Expression],
      op: TypedColumn[X1[A], A] => TypedColumn[X1[A], Boolean]
    ): Assertion = {
    withDataset(expected) { dataset =>
      val ds = dataset.filter(op(dataset('a)))
      val actualPushDownFilters = pushDownFilters(ds)

      val optimizedPlan = ds.queryExecution.optimizedPlan.collect {
        case logical.Filter(condition, _) => condition
      }.flatMap(_.toList)

      // check the optimized plan
      optimizedPlan.collectFirst(planShouldContain) should not be (empty)

      // compare filters
      actualPushDownFilters shouldBe expectedPushDownFilters

      val actual = ds.collect().run().toVector.headOption

      // ensure serialization is not broken
      actual should be(Some(expected))
    }
  }

}
