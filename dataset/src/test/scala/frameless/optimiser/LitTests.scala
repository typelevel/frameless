package frameless.optimiser

import frameless._
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec

class LitTests extends TypedDatasetSuite {
  test("sqlTimestamp pushdown") {
    val ms = SQLTimestamp(System.currentTimeMillis())
    TypedDataset.create(Seq(X1(ms))).write.mode("overwrite").parquet("./target/testData")
    val dataset = TypedDataset.createUnsafe[X1[SQLTimestamp]](session.read.parquet("./target/testData"))

    {
      val pushDowns = getPushDowns(dataset.filter(dataset('a) >= ms))
      assert(!pushDowns.exists(_.contains("GreaterThanOrEqual")), "Should NOT have had a push down predicate for the underlying long as no optimiser is yet used")
    }

    val orig = session.sqlContext.experimental.extraOptimizations
    try {
      session.sqlContext.experimental.extraOptimizations ++= Seq(LiteralRule)

      val pushDowns = getPushDowns(dataset.filter(dataset('a) >= ms))
      assert(pushDowns.exists(_.contains("GreaterThanOrEqual")), "Should have had a push down predicate for the underlying long")
    } finally {
      session.sqlContext.experimental.extraOptimizations = orig
    }
  }

  def getPushDowns(dataset: TypedDataset[_]): Seq[String] = {
    val sparkPlan = dataset.queryExecution.executedPlan

    (if (sparkPlan.children.isEmpty)
    // assume it's AQE
      sparkPlan match {
        case aq: AdaptiveSparkPlanExec => aq.initialPlan
        case _ => sparkPlan
      }
    else
      sparkPlan).collect {
      case fs: FileSourceScanExec =>
        fs.metadata.collect { case ("PushedFilters", value) if value != "[]" => value }
    }.flatten
  }
}
