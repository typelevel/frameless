package frameless.sql.rules

import frameless._
import frameless.sql._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.catalyst.plans.logical
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers

trait SQLRulesSuite extends TypedDatasetSuite with Matchers { self =>
  protected lazy val path: String = {
    val tmpDir = System.getProperty("java.io.tmpdir")
    s"$tmpDir/${self.getClass.getName}"
  }

  def withDataset[A: TypedEncoder: CatalystOrdered](payload: A)(f: TypedDataset[A] => Assertion): Assertion = {
    TypedDataset.create(Seq(payload)).write.mode("overwrite").parquet(path)
    f(TypedDataset.createUnsafe[A](session.read.parquet(path)))
  }

  def optimizationRulesTest[A: TypedEncoder : CatalystOrdered](
    expected: X1[A],
    expectedPushDownFilters: List[Filter],
    planShouldNotContain: PartialFunction[Expression, Expression],
    op: TypedColumn[X1[A], A] => TypedColumn[X1[A], Boolean]
  ): Assertion = {
    withDataset(expected) { dataset =>
      val ds = dataset.filter(op(dataset('a)))
      val actualPushDownFilters = ds.pushDownFilters

      val optimizedPlan = ds.queryExecution.optimizedPlan.collect { case logical.Filter(condition, _) => condition }.flatMap(_.toList)

      // check the optimized plan
      optimizedPlan.collectFirst(planShouldNotContain) should be (empty)

      // compare filters
      actualPushDownFilters shouldBe expectedPushDownFilters

      val actual = ds.collect().run().toVector.headOption
      // ensure serialization is not broken
      actual should be(Some(expected))
    }
  }

  def noOptimizationRulesTest[A: TypedEncoder : CatalystOrdered](
    expected: X1[A],
    planShouldContain: PartialFunction[Expression, Expression],
    op: TypedColumn[X1[A], A] => TypedColumn[X1[A], Boolean]
  ): Assertion = {
    withDataset(expected) { dataset =>
      val ds = dataset.filter(op(dataset('a)))

      val optimizedPlan = ds.queryExecution.optimizedPlan.collect { case logical.Filter(condition, _) => condition }.flatMap(_.toList)

      // check the optimized plan
      optimizedPlan.collectFirst(planShouldContain) shouldNot be(empty)
    }
  }
}
