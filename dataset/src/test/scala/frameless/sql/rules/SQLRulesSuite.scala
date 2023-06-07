package frameless.sql.rules

import frameless._
import frameless.sql._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec

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

  def predicatePushDownTest[A: TypedEncoder: CatalystOrdered](
    expected: X1[A],
    expectedPushDownFilters: List[Filter],
    planShouldNotContain: PartialFunction[Expression, Expression],
    op: TypedColumn[X1[A], A] => TypedColumn[X1[A], Boolean]
  ): Assertion = {
    withDataset(expected) { dataset =>
      val ds = dataset.filter(op(dataset('a)))
      val actualPushDownFilters = pushDownFilters(ds)

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

  protected def pushDownFilters[T](ds: TypedDataset[T]): List[Filter] = {
    val sparkPlan = ds.queryExecution.executedPlan

    val initialPlan =
      if (sparkPlan.children.isEmpty) // assume it's AQE
        sparkPlan match {
          case aq: AdaptiveSparkPlanExec => aq.initialPlan
          case _ => sparkPlan
        }
      else
        sparkPlan

    initialPlan.collect {
      case fs: FileSourceScanExec =>
        import scala.reflect.runtime.{universe => ru}

        val runtimeMirror = ru.runtimeMirror(getClass.getClassLoader)
        val instanceMirror = runtimeMirror.reflect(fs)
        val getter = ru.typeOf[FileSourceScanExec].member(ru.TermName("pushedDownFilters")).asTerm.getter
        val m = instanceMirror.reflectMethod(getter.asMethod)
        val res = m.apply(fs).asInstanceOf[Seq[Filter]]

        res
    }.flatten.toList
  }
}
